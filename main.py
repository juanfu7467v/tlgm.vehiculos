"""
╔══════════════════════════════════════════════════════════════════╗
║         API Gateway  /seeker  v5.0  —  Full Capture             ║
║                                                                  ║
║  BUGS CORREGIDOS vs v4.0:                                        ║
║                                                                  ║
║  BUG #1 (CRÍTICO):                                               ║
║    _detect_section() se llamaba sobre el texto YA LIMPIADO.      ║
║    La limpieza borraba la línea completa:                        ║
║      "[#LEDER_BOT] → SEEKER | TELEFONOS [PREMIUM]"              ║
║    Sin esa línea, telefonos/sueldos/correos quedaban como        ║
║    "otro" sin clasificar correctamente.                          ║
║  SOLUCIÓN: detectar sección desde el texto RAW (antes de        ║
║    limpiar), luego limpiar solo para extraer los campos.         ║
║                                                                  ║
║  BUG #2:                                                         ║
║    Si el mensaje de foto tenía caption (texto adjunto), ese      ║
║    texto se descartaba silenciosamente.                          ║
║  SOLUCIÓN: si has_media=True y raw_text no está vacío,           ║
║    procesar el texto como una sección de datos adicional.        ║
║                                                                  ║
║  MEJORA: logging detallado con primeros 120 chars de cada        ║
║    mensaje → visible en fly.io logs para diagnóstico.            ║
╚══════════════════════════════════════════════════════════════════╝
"""

import os
import re
import sys
import atexit
import signal
import asyncio
import traceback
import time
import mimetypes
import threading
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto
from telethon.errors.rpcerrorlist import UserBlockedError

# ══════════════════════════════════════════════════════════════════
#  VARIABLES DE ENTORNO
# ══════════════════════════════════════════════════════════════════
API_ID         = int(os.getenv("API_ID", "0"))
API_HASH       = os.getenv("API_HASH", "")
PUBLIC_URL     = os.getenv("PUBLIC_URL", "").rstrip("/")
SESSION_STRING = os.getenv("SESSION_STRING", None)
PORT           = int(os.getenv("PORT", 8080))

# ══════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN INTERNA
# ══════════════════════════════════════════════════════════════════
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

BOT_PRIMARY     = "@LEDERDATA_OFC_BOT"
BOT_BACKUP      = "@lederdata_publico_bot"
ALL_BOTS        = [BOT_PRIMARY, BOT_BACKUP]

TIMEOUT_PRIMARY = 60
TIMEOUT_BACKUP  = 90
SILENCE_SECS    = 7.0
BOT_BLOCK_HOURS = 1

THREAD_POOL = ThreadPoolExecutor(max_workers=8, thread_name_prefix="seeker_worker")
APP_VERSION = "5.0"
STARTED_AT_UTC = datetime.utcnow()
HEALTHCHECK_PATHS = {"/", "/health", "/status", "/debug"}
_clean_storage_lock = threading.Lock()
_shutdown_lock = threading.Lock()
_shutdown_started = False

# ══════════════════════════════════════════════════════════════════
#  ESTADO GLOBAL
# ══════════════════════════════════════════════════════════════════
_bot_fail_tracker: dict = {}
_in_flight: dict        = {}
_in_flight_lock         = threading.Lock()


def _bot_blocked(bot_id: str) -> bool:
    ts = _bot_fail_tracker.get(bot_id)
    if not ts:
        return False
    if ts > datetime.now() - timedelta(hours=BOT_BLOCK_HOURS):
        return True
    _bot_fail_tracker.pop(bot_id, None)
    print(f"✅ Bot {bot_id} desbloqueado")
    return False


def _mark_bot_failed(bot_id: str):
    print(f"🚨 Bot {bot_id} bloqueado {BOT_BLOCK_HOURS}h")
    _bot_fail_tracker[bot_id] = datetime.now()


# ══════════════════════════════════════════════════════════════════
#  CICLO DE VIDA / APAGADO GRACEFUL
# ══════════════════════════════════════════════════════════════════
def _shutdown_resources():
    global _shutdown_started
    with _shutdown_lock:
        if _shutdown_started:
            return
        _shutdown_started = True

    print("🛑 Apagando recursos de la aplicación...")
    try:
        THREAD_POOL.shutdown(wait=False, cancel_futures=True)
    except TypeError:
        THREAD_POOL.shutdown(wait=False)
    except Exception as e:
        print(f"⚠️  Error cerrando THREAD_POOL: {e}")


def _signal_handler(signum, frame):
    print(f"🛑 Señal de apagado recibida: {signum}")
    _shutdown_resources()
    raise SystemExit(0)


atexit.register(_shutdown_resources)
for _sig in (signal.SIGTERM, signal.SIGINT):
    try:
        signal.signal(_sig, _signal_handler)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════
#  VALIDACIÓN DE DNI
# ══════════════════════════════════════════════════════════════════
def validate_dni(raw: str):
    if not raw:
        return None, "Parametro 'dni' requerido. Ejemplo: ?dni=43652762"
    raw = raw.strip()
    if not raw.isdigit():
        return None, f"DNI invalido '{raw}': solo digitos numericos."
    if len(raw) != 8:
        return None, f"DNI invalido '{raw}': debe tener exactamente 8 digitos (recibidos {len(raw)})."
    return raw, None


# ══════════════════════════════════════════════════════════════════
#  IDENTIFICADOR DE SECCIÓN
#
#  ⚠️  IMPORTANTE: esta función SIEMPRE se llama con el texto RAW
#  (ANTES de limpiar), porque la limpieza elimina los headers de
#  sección como "[#LEDER_BOT] → SEEKER | TELEFONOS [PREMIUM]".
# ══════════════════════════════════════════════════════════════════
def _detect_section(raw_text: str) -> str:
    """
    Detecta la sección del mensaje a partir del texto ORIGINAL (sin limpiar).

    Secciones conocidas del bot LEDER DATA:
      datos         → SEEKER [PREMIUM]          (datos personales)
      direcciones   → SEEKER | DIRECCIONES
      correos       → SEEKER | CORREOS
      sueldos       → SEEKER | SUELDOS
      telefonos     → SEEKER | TELEFONOS
      familiares    → SEEKER | FAMILIARES
      vehiculos     → SEEKER | VEHICULOS
      judicial      → SEEKER | JUDICIAL
      redes_sociales→ SEEKER | REDES
      otro          → sección no reconocida pero con datos
      vacio         → sin datos útiles
    """
    t = raw_text.upper()

    # Secciones compuestas primero (más específicas)
    if re.search(r"SEEKER\s*\|\s*DIRECCIONES|DIRECCIONES\s*\[PREMIUM\]", t):
        return "direcciones"
    if re.search(r"SEEKER\s*\|\s*CORREOS|CORREOS\s*\[PREMIUM\]", t):
        return "correos"
    if re.search(r"SEEKER\s*\|\s*SUELDOS|SUELDOS\s*\[PREMIUM\]", t):
        return "sueldos"
    if re.search(r"SEEKER\s*\|\s*TELEFONOS?|TELEFONOS?\s*\[PREMIUM\]", t):
        return "telefonos"
    if re.search(r"SEEKER\s*\|\s*FAMILIARES?|FAMILIARES?\s*\[PREMIUM\]", t):
        return "familiares"
    if re.search(r"SEEKER\s*\|\s*VEHICULOS?|VEHICULOS?\s*\[PREMIUM\]", t):
        return "vehiculos"
    if re.search(r"SEEKER\s*\|\s*JUDICIAL|JUDICIAL\s*\[PREMIUM\]", t):
        return "judicial"
    if re.search(r"SEEKER\s*\|\s*REDES|REDES\s*\[PREMIUM\]", t):
        return "redes_sociales"
    if re.search(r"SEEKER\s*\|\s*NACIMIENTO|NACIMIENTO\s*\[PREMIUM\]", t):
        return "datos"  # NACIMIENTO viene junto a datos personales

    # Sección principal de datos personales
    if re.search(r"SEEKER\s*\[PREMIUM\]", t):
        return "datos"

    # Si no tiene cabecera SEEKER pero tiene pares CLAVE : VALOR → "otro"
    if ":" in raw_text:
        return "otro"

    return "vacio"


# ══════════════════════════════════════════════════════════════════
#  LIMPIEZA DE TEXTO
#  Solo elimina marcas del bot. NO toca datos del ciudadano.
#  Se aplica DESPUÉS de detectar la sección.
# ══════════════════════════════════════════════════════════════════
_BOT_META_RE = re.compile(
    # Línea completa que empieza con [#LEDER_BOT]
    r"^\s*\[#?LEDER_BOT\].*$"
    # Línea completa que empieza con [CONSULTA PE]
    r"|^\s*\[CONSULTA\s*PE\].*$"
    # Línea completa que empieza con → SEEKER (sin [#LEDER_BOT])
    r"|^\s*→\s*SEEKER[^\n]*$"
    # Línea de créditos: Credits : 3788
    r"|^\s*Credits\s*:\s*\d+[^\n]*$"
    # Línea de Wanted for
    r"|^\s*Wanted\s+for\s*:[^\n]*$"
    # Línea de separadores ---
    r"|^\s*-{3,}\s*$"
    # "Se encontro X resultados" — es metadata, no dato del ciudadano
    r"|^\s*Se\s+encontro\s+\d+\s+resultados?\.?\s*$",
    re.IGNORECASE | re.MULTILINE,
)


def _clean_for_parsing(raw_text: str) -> str:
    """
    Limpia el texto para extracción de campos:
    - Elimina headers/footers del bot.
    - Elimina "Se encontro X resultados".
    - Normaliza espacios.
    - Preserva todos los datos CLAVE : VALOR del ciudadano.
    """
    if not raw_text:
        return ""
    t = _BOT_META_RE.sub("", raw_text)
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


# ══════════════════════════════════════════════════════════════════
#  PARSER UNIVERSAL DINÁMICO
#
#  Convierte CUALQUIER "CLAVE : VALOR" en campo JSON.
#  Sin lista fija → nunca se pierde ningún campo nuevo del bot.
# ══════════════════════════════════════════════════════════════════
def _normalize_key(raw_key: str) -> str:
    """
    "APELLIDO PATERNO" → "apellido_paterno"
    "FECHA NACIMIENTO" → "fecha_nacimiento"
    """
    key = raw_key.strip().lower()
    key = re.sub(r"[^a-z0-9áéíóúñü]+", "_", key)
    return key.strip("_")


def _parse_kv_line(line: str):
    """
    Parsea "CLAVE : VALOR" de una línea.
    Retorna (key_normalizada, valor) o None.
    """
    m = re.match(r"^([^:\n]{1,60}?)\s*:\s*(.+)$", line.strip())
    if not m:
        return None
    raw_key = m.group(1).strip()
    raw_val = m.group(2).strip()

    if not raw_key or len(raw_key) < 2:
        return None
    if raw_key.isdigit():
        return None
    if "http" in raw_key.lower():
        return None

    return _normalize_key(raw_key), raw_val


# Claves de metadata del bot a ignorar en los datos finales
_META_KEYS = frozenset({
    "credits", "wanted_for", "leder_bot", "consulta_pe",
    "se_encontro", "resultados", "pagina",
})


def _text_to_dict(text: str) -> dict:
    """
    Convierte un bloque de texto en diccionario.
    Procesa TODAS las líneas con CLAVE : VALOR.
    Si una clave se repite, guarda lista.
    """
    result = {}
    if not text:
        return result

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parsed = _parse_kv_line(line)
        if not parsed:
            continue
        key, val = parsed
        if key in _META_KEYS:
            continue
        if not val:
            continue
        if key in result:
            existing = result[key]
            if isinstance(existing, list):
                if val not in existing:
                    existing.append(val)
            elif existing != val:
                result[key] = [existing, val]
        else:
            result[key] = val

    return result


def _has_not_found(text: str) -> bool:
    return bool(re.search(
        r"no se encontr[oó]|no se han encontrado|no hay resultados|"
        r"no tenemos datos|sin resultados|0 resultados",
        text, re.IGNORECASE
    ))


# ══════════════════════════════════════════════════════════════════
#  DIVISOR DE BLOQUES
#  Separa un mensaje con múltiples registros en bloques individuales.
# ══════════════════════════════════════════════════════════════════
def _split_into_blocks(text: str) -> list:
    """
    Divide texto con múltiples registros en bloques.
    Detecta separadores por doble salto de línea o por inicio de
    un nuevo registro (campo identificador repetido).
    """
    # División por doble salto de línea
    bloques = re.split(r"\n\s*\n", text)

    # Si quedó un solo bloque, intentar división por campo identificador
    if len(bloques) <= 1:
        bloques = re.split(
            r"(?=\nDOCUMENTO\s*:|\nCORREO\s*:|\nRUC\s*:"
            r"|\nTELEFONO\s*:|\nPLACA\s*:|\nNOMBRES\s*:)",
            text
        )

    result = []
    for b in bloques:
        b = b.strip()
        if b and ":" in b and len(b) > 5:
            result.append(b)

    return result if result else [text]


# ══════════════════════════════════════════════════════════════════
#  PROCESADOR DE MENSAJE  (ejecutado en hilo)
#
#  FIX PRINCIPAL: _detect_section() se llama con raw_text
#  ANTES de la limpieza, para no perder los headers de sección.
# ══════════════════════════════════════════════════════════════════
def _process_message_in_thread(msg: dict) -> dict:
    """
    Procesa un mensaje completamente en un hilo del pool.

    Flujo corregido:
      1. Detectar sección desde raw_text (ANTES de limpiar).
      2. Limpiar el texto.
      3. Dividir en bloques.
      4. Parsear todos los campos CLAVE : VALOR.

    Para mensajes con media + caption:
      Se procesa TANTO la foto como el texto del caption.
    """
    raw_text  = msg.get("raw_text", "")
    has_media = msg.get("has_media", False)
    urls      = msg.get("urls", [])

    # ── Mensaje de foto ────────────────────────────────────────────────────────
    if has_media:
        foto_url = urls[0].get("url") if urls else None

        # FIX BUG #2: Si la foto tiene caption (texto adjunto), procesarlo también
        caption_result = None
        if raw_text and raw_text.strip():
            # El caption puede contener la sección de datos personales
            section_cap = _detect_section(raw_text)
            clean_cap   = _clean_for_parsing(raw_text)
            if clean_cap and section_cap != "vacio":
                bloques     = _split_into_blocks(clean_cap)
                registros   = [
                    r for b in bloques
                    if (r := {k: v for k, v in _text_to_dict(b).items()
                               if k not in _META_KEYS and v})
                ]
                if registros:
                    caption_result = {
                        "section":   section_cap,
                        "registros": registros,
                    }

        return {
            "section":        "foto",
            "registros":      [],
            "foto_url":       foto_url,
            "raw_text":       "",
            "caption_result": caption_result,
        }

    # ── Mensaje de texto vacío ─────────────────────────────────────────────────
    if not raw_text or not raw_text.strip():
        return {"section": "vacio", "registros": [], "foto_url": None, "raw_text": ""}

    # ── FIX BUG #1: Detectar sección desde RAW text (antes de limpiar) ─────────
    section = _detect_section(raw_text)

    # ── Limpiar DESPUÉS de detectar sección ────────────────────────────────────
    clean = _clean_for_parsing(raw_text)

    # Si después de limpiar no queda nada útil
    if not clean:
        return {"section": "vacio", "registros": [], "foto_url": None, "raw_text": ""}

    # Dividir en bloques individuales
    bloques = _split_into_blocks(clean)

    # Parsear cada bloque
    registros = []
    for bloque in bloques:
        fields = _text_to_dict(bloque)
        campos = {k: v for k, v in fields.items() if k not in _META_KEYS and v}
        if campos:
            registros.append(campos)

    return {
        "section":        section,
        "registros":      registros,
        "foto_url":       None,
        "raw_text":       clean,
        "caption_result": None,
    }


# ══════════════════════════════════════════════════════════════════
#  CONSOLIDADOR MULTIHILO
#  Procesa todos los mensajes en paralelo y construye el JSON final.
# ══════════════════════════════════════════════════════════════════
SECCIONES_LISTA = frozenset({
    "direcciones", "correos", "sueldos", "telefonos",
    "familiares",  "vehiculos", "judicial", "redes_sociales",
})


def consolidate_multithreaded(captured_msgs: list) -> dict:
    """
    Envía cada mensaje al pool de hilos para procesarlo en paralelo.
    Consolida todos los resultados en un único JSON estructurado.
    """
    if not captured_msgs:
        return {"status": "success", "total_mensajes": 0}

    n = len(captured_msgs)
    print(f"⚙️  Procesando {n} msgs en {THREAD_POOL._max_workers} hilos...")
    t0 = time.time()

    futures_map = {
        THREAD_POOL.submit(_process_message_in_thread, msg): idx
        for idx, msg in enumerate(captured_msgs)
    }

    ordered = [None] * n
    for future in as_completed(futures_map, timeout=30):
        idx = futures_map[future]
        try:
            ordered[idx] = future.result()
        except Exception as e:
            print(f"⚠️  Error hilo {idx}: {e}")
            ordered[idx] = {
                "section": "vacio", "registros": [], "foto_url": None,
                "raw_text": "", "caption_result": None,
            }

    elapsed = time.time() - t0
    print(f"⚙️  Procesamiento paralelo: {elapsed:.3f}s")

    # ── Resultado inicial ──────────────────────────────────────────────────────
    result = {
        "status":         "success",
        "total_mensajes": n,
        "foto_url":       None,
        "datos":          {},
        "direcciones":    [],
        "correos":        [],
        "sueldos":        [],
        "telefonos":      [],
        "familiares":     [],
        "vehiculos":      [],
        "judicial":       [],
        "redes_sociales": [],
        "otros":          [],
    }

    def _merge_into_datos(registros):
        """Fusiona registros en result['datos'] sin sobrescribir."""
        for reg in registros:
            for k, v in reg.items():
                if k not in result["datos"]:
                    result["datos"][k] = v
                else:
                    existing = result["datos"][k]
                    if isinstance(existing, list):
                        if v not in existing:
                            existing.append(v)
                    elif existing != v:
                        result["datos"][k] = [existing, v]

    # ── Consolidar resultados en orden de llegada ──────────────────────────────
    for parsed in ordered:
        if not parsed:
            continue

        section        = parsed.get("section", "vacio")
        registros      = parsed.get("registros", [])
        foto_url       = parsed.get("foto_url")
        caption_result = parsed.get("caption_result")

        # Foto principal
        if section == "foto":
            if foto_url:
                result["foto_url"] = foto_url
            # FIX BUG #2: procesar caption si existe
            if caption_result:
                cap_sec  = caption_result.get("section", "otro")
                cap_regs = caption_result.get("registros", [])
                if cap_sec == "datos":
                    _merge_into_datos(cap_regs)
                elif cap_sec in SECCIONES_LISTA:
                    result[cap_sec].extend(cap_regs)
                elif cap_sec not in ("vacio", "foto") and cap_regs:
                    for r in cap_regs:
                        result["otros"].append({"seccion": cap_sec, **r})

        # Datos personales
        elif section == "datos":
            _merge_into_datos(registros)

        # Secciones en lista
        elif section in SECCIONES_LISTA:
            result[section].extend(registros)

        # Sección "otro" — sin header conocido pero con datos
        elif section == "otro" and registros:
            for r in registros:
                result["otros"].append(r)

        # "vacio" → ignorar

    # ── Limpiar claves vacías ──────────────────────────────────────────────────
    for k in list(SECCIONES_LISTA) + ["otros"]:
        if k in result and not result[k]:
            del result[k]

    if not result.get("foto_url"):
        result.pop("foto_url", None)

    if not result.get("datos"):
        result.pop("datos", None)

    # ── Log del resumen para diagnóstico ──────────────────────────────────────
    secciones_con_datos = [
        k for k in result
        if k not in ("status", "total_mensajes")
        and result[k]
    ]
    print(f"📊 Resultado: {secciones_con_datos}")

    return result


# ══════════════════════════════════════════════════════════════════
#  NÚCLEO ASÍNCRONO
# ══════════════════════════════════════════════════════════════════
async def _execute_seeker(command: str, consulta_id: str) -> dict:
    """
    1. Conectar Telegram.
    2. Enviar /seeker <dni> al bot (principal → respaldo si falla).
    3. Capturar TODOS los mensajes con silencio dinámico.
    4. Descargar fotos.
    5. Procesar en paralelo con consolidate_multithreaded().
    6. Retornar JSON completo.
    """
    client          = None
    handler_ref     = [None]
    handler_removed = False

    try:
        if API_ID == 0 or not API_HASH:
            raise Exception("API_ID o API_HASH no configurados.")
        if not SESSION_STRING or not SESSION_STRING.strip():
            raise Exception("SESSION_STRING no configurada.")

        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        await client.connect()
        if not await client.is_user_authorized():
            raise Exception("Sesion de Telegram expirada o invalida.")

        bots = []
        if not _bot_blocked(BOT_PRIMARY):
            bots.append((BOT_PRIMARY, TIMEOUT_PRIMARY))
        if not _bot_blocked(BOT_BACKUP):
            bots.append((BOT_BACKUP,  TIMEOUT_BACKUP))
        if not bots:
            raise Exception("Todos los bots bloqueados temporalmente.")

        print(f"🤖 Bots disponibles: {[b[0] for b in bots]}")

        captured_msgs: list = []
        stop_event          = asyncio.Event()
        last_msg_ts         = [time.time()]
        active_bot          = [None]

        # ── Handler ────────────────────────────────────────────────────────────
        @client.on(events.NewMessage(incoming=True))
        async def _handler(event):
            if stop_event.is_set():
                return
            try:
                if not active_bot[0]:
                    return
                entity = await client.get_entity(active_bot[0])
                if event.sender_id != entity.id:
                    return

                last_msg_ts[0] = time.time()
                raw_text  = event.raw_text or ""
                has_media = bool(getattr(event.message, "media", None))

                msg_obj = {
                    "raw_text":      raw_text,
                    "has_media":     has_media,
                    "urls":          [],
                    "event_message": event.message,
                }
                captured_msgs.append(msg_obj)

                # Log detallado para diagnóstico (primeros 120 chars)
                preview = repr(raw_text[:120]).replace("'", "") if raw_text else ""
                tipo    = f"📷+{len(raw_text)}c" if has_media and raw_text else \
                          "📷 foto" if has_media else f"📝 {len(raw_text)}c"
                bot_tag = "RESP" if active_bot[0] == BOT_BACKUP else "PRIN"
                print(f"   📥 Msg #{len(captured_msgs)} [{tipo}] ← {bot_tag} | {preview}")

            except Exception as e:
                print(f"   ⚠️  Handler error: {e}")

        handler_ref[0] = _handler

        # ── Loop de bots ───────────────────────────────────────────────────────
        for bot_id, timeout_secs in bots:
            is_primary = (bot_id == BOT_PRIMARY)
            label      = "BOT PRINCIPAL" if is_primary else "BOT RESPALDO"

            captured_msgs.clear()
            stop_event.clear()
            last_msg_ts[0] = time.time()
            active_bot[0]  = bot_id

            print(f"\n{'─'*58}")
            print(f"🎯 {label}: {bot_id}")
            print(f"   Comando : {command}")
            print(f"   Timeout : {timeout_secs}s  |  Silencio: {SILENCE_SECS}s")
            print(f"{'─'*58}")

            try:
                await client.send_message(bot_id, command)
                print(f"✅ Enviado a {label}")
            except UserBlockedError:
                print(f"❌ {label} bloqueado")
                _mark_bot_failed(bot_id)
                continue
            except Exception as e:
                print(f"❌ Error enviando: {str(e)[:100]}")
                if "blocked" in str(e).lower():
                    _mark_bot_failed(bot_id)
                continue

            t0             = time.time()
            first_msg_time = [None]

            while True:
                elapsed = time.time() - t0
                silence = time.time() - last_msg_ts[0]

                if captured_msgs:
                    if first_msg_time[0] is None:
                        first_msg_time[0] = time.time()
                        print(f"   ⏱️  1er mensaje en {first_msg_time[0]-t0:.1f}s")
                    if silence >= SILENCE_SECS:
                        print(
                            f"   ✅ Silencio {silence:.1f}s → "
                            f"{len(captured_msgs)} msgs capturados "
                            f"({elapsed:.1f}s total)"
                        )
                        break

                if elapsed > timeout_secs:
                    print(f"   ⏰ Timeout {timeout_secs}s | {len(captured_msgs)} msgs")
                    break

                await asyncio.sleep(0.15)

            if not captured_msgs:
                if is_primary:
                    print(f"❌ Principal sin respuesta → bloqueando {BOT_BLOCK_HOURS}h")
                    _mark_bot_failed(BOT_PRIMARY)
                else:
                    print("❌ Respaldo sin respuesta")
                    _mark_bot_failed(BOT_BACKUP)
                    raise Exception("Bot de respaldo no respondio.")
                continue

            if is_primary and any(
                "ANTI-SPAM" in (m.get("raw_text") or "").upper()
                for m in captured_msgs
            ):
                print("🔄 ANTI-SPAM → cambiando a respaldo (5s)...")
                await asyncio.sleep(5)
                continue

            stop_event.set()
            print(f"\n✅ {len(captured_msgs)} mensajes capturados → procesando...")
            break

        else:
            raise Exception("No se recibio respuesta valida de ningun bot.")

        # ── Descargar fotos ─────────────────────────────────────────────────────
        print("📥 Descargando multimedia...")
        for idx, msg in enumerate(captured_msgs):
            event_msg = msg.get("event_message")
            if not event_msg or not getattr(event_msg, "media", None):
                continue
            try:
                media        = event_msg.media
                file_ext     = ".jpg"
                content_type = "image/jpeg"

                if isinstance(media, MessageMediaPhoto):
                    file_ext, content_type = ".jpg", "image/jpeg"
                elif isinstance(media, MessageMediaDocument):
                    mime         = getattr(getattr(media, "document", None), "mime_type", "") or ""
                    file_ext     = mimetypes.guess_extension(mime) or ".bin"
                    content_type = mime or "application/octet-stream"

                ts    = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
                fname = f"seeker_{ts}_{idx}{file_ext}"
                fpath = os.path.join(DOWNLOAD_DIR, fname)

                if not client.is_connected():
                    await client.connect()

                saved = await client.download_media(event_msg, file=fpath)
                if saved and os.path.exists(saved):
                    url              = f"{PUBLIC_URL}/files/{os.path.basename(saved)}"
                    msg["urls"]      = [{"url": url, "type": content_type}]
                    msg["has_media"] = True
                    print(f"   ✅ {os.path.basename(saved)}")

            except Exception as e:
                print(f"   ⚠️  Error descargando msg {idx}: {e}")

        # Remover handler
        try:
            client.remove_event_handler(handler_ref[0])
            handler_removed = True
        except Exception:
            pass

        # ── Verificaciones ─────────────────────────────────────────────────────
        for m in captured_msgs:
            if "Por favor, usa el formato correcto" in (m.get("raw_text") or ""):
                return {"status": "error", "message": "Formato de consulta incorrecto."}

        text_msgs = [
            m for m in captured_msgs
            if not m.get("has_media") and len(m.get("raw_text", "")) > 20
        ]
        if text_msgs and all(_has_not_found(m.get("raw_text", "")) for m in text_msgs):
            return {"status": "error", "message": "No se encontraron resultados para este DNI."}

        # ── Consolidar ─────────────────────────────────────────────────────────
        return consolidate_multithreaded(captured_msgs)

    except Exception as e:
        print(f"❌ Error en _execute_seeker: {e}")
        traceback.print_exc()
        return {"status": "error", "message": f"Error al procesar: {str(e)}"}

    finally:
        if client:
            try:
                if not handler_removed and handler_ref[0]:
                    try:
                        client.remove_event_handler(handler_ref[0])
                    except Exception:
                        pass
                await client.disconnect()
                print("🔌 Cliente desconectado")
            except Exception as e:
                print(f"⚠️  Error desconectando: {e}")

        try:
            now = time.time()
            cnt = 0
            for fn in os.listdir(DOWNLOAD_DIR):
                fp = os.path.join(DOWNLOAD_DIR, fn)
                if os.path.isfile(fp) and now - os.path.getmtime(fp) > 300:
                    os.remove(fp)
                    cnt += 1
            if cnt:
                print(f"🧹 {cnt} archivos temporales eliminados")
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════
#  WRAPPER SÍNCRONO + DEDUPLICACIÓN
# ══════════════════════════════════════════════════════════════════
def run_seeker(command: str, consulta_id: str) -> dict:
    from concurrent.futures import Future as ConcFuture

    dedup_key = command.strip()

    with _in_flight_lock:
        existing = _in_flight.get(dedup_key)
        if existing is None:
            fut = ConcFuture()
            _in_flight[dedup_key] = fut
        else:
            fut = None

    if existing is not None:
        print(f"⚡ Peticion duplicada '{dedup_key}' → esperando resultado")
        try:
            return existing.result(timeout=TIMEOUT_PRIMARY + TIMEOUT_BACKUP + 15)
        except Exception:
            pass
        with _in_flight_lock:
            fut = ConcFuture()
            _in_flight[dedup_key] = fut

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(_execute_seeker(command, consulta_id))
        fut.set_result(result)
        return result
    except Exception as e:
        fut.set_exception(e)
        raise
    finally:
        loop.close()
        with _in_flight_lock:
            _in_flight.pop(dedup_key, None)


# ══════════════════════════════════════════════════════════════════
#  LIMPIEZA DE ALMACENAMIENTO
#  Ejecutada en cada petición para evitar que el disco (512MB) se llene.
# ══════════════════════════════════════════════════════════════════
MAX_STORAGE_MB     = 400   # Umbral de limpieza (margen sobre los 512MB del VM)
MAX_FILE_AGE_SECS  = 3600  # Archivos con > 1 hora de antigüedad se eliminan siempre


def clean_storage():
    """
    Limpia el directorio de descargas temporales en dos pasadas:
      1. Elimina todos los archivos con más de MAX_FILE_AGE_SECS de antigüedad.
      2. Si el uso total sigue superando MAX_STORAGE_MB, borra los más
         antiguos primero hasta quedar por debajo del umbral.

    La limpieza está protegida con lock para evitar que varias peticiones
    compitan entre sí durante los chequeos de salud o en ráfagas de tráfico.
    """
    if not _clean_storage_lock.acquire(blocking=False):
        return

    try:
        now = time.time()
        files_info = []

        for fname in os.listdir(DOWNLOAD_DIR):
            fpath = os.path.join(DOWNLOAD_DIR, fname)
            if not os.path.isfile(fpath):
                continue
            files_info.append((fpath, os.path.getmtime(fpath), os.path.getsize(fpath)))

        # ── Pasada 1: eliminar por antigüedad ──────────────────────────────────
        removed = 0
        for fpath, mtime, _ in files_info:
            if now - mtime > MAX_FILE_AGE_SECS:
                try:
                    os.remove(fpath)
                    removed += 1
                except Exception:
                    pass
        if removed:
            print(f"🧹 clean_storage: {removed} archivos eliminados por antigüedad")

        # ── Pasada 2: eliminar por tamaño total si sigue alto ──────────────────
        remaining = [
            (fpath, mtime, size)
            for fpath, mtime, size in files_info
            if os.path.isfile(fpath)
        ]
        total_mb = sum(s for _, _, s in remaining) / (1024 * 1024)

        if total_mb > MAX_STORAGE_MB:
            remaining.sort(key=lambda x: x[1])  # más antiguos primero
            for fpath, _, size in remaining:
                if total_mb <= MAX_STORAGE_MB:
                    break
                try:
                    os.remove(fpath)
                    total_mb -= size / (1024 * 1024)
                    print(f"🧹 clean_storage: eliminado por cuota → {os.path.basename(fpath)}")
                except Exception:
                    pass

    except Exception as e:
        print(f"[clean_storage] Error: {e}")
    finally:
        _clean_storage_lock.release()


# ══════════════════════════════════════════════════════════════════
#  FLASK APPLICATION
# ══════════════════════════════════════════════════════════════════
app = Flask(__name__)
CORS(app)


@app.before_request
def _before_each_request():
    """
    Registra la petición y evita que los chequeos de salud / wake-up queden
    bloqueados por tareas no esenciales.
    """
    request.environ["request_started_at"] = time.time()

    if request.path == "/seeker":
        clean_storage()




@app.after_request
def _after_each_request(response):
    started = request.environ.get("request_started_at")
    duration_ms = 0
    if started is not None:
        duration_ms = int((time.time() - started) * 1000)

    print(
        f"🌐 {request.method} {request.path} -> {response.status_code} "
        f"({duration_ms}ms) from {request.headers.get('Fly-Client-IP', request.remote_addr)}"
    )
    return response


@app.errorhandler(Exception)
def _handle_unexpected_error(e):
    traceback.print_exc()
    return jsonify({
        "status": "error",
        "message": f"Error interno no controlado: {str(e)}",
    }), 500

@app.route("/", methods=["GET"])
def root():
    return jsonify({
        "status":  "ok",
        "service": "API Gateway /seeker v5.0",
        "version": APP_VERSION,
        "uso":     "GET /seeker?dni=43652762",
        "wake_ready": True,
        "uptime_seconds": int((datetime.utcnow() - STARTED_AT_UTC).total_seconds()),
        "fixes":   [
            "v5: _detect_section() usa raw_text (antes de limpiar)",
            "v5: Caption de foto ahora se procesa",
            "v5: Log detallado con preview de cada mensaje",
            "v5.0.1: wake-up estable en Fly.io y logging de requests",
        ],
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status":              "healthy",
        "version":             APP_VERSION,
        "timestamp":           datetime.utcnow().isoformat(),
        "uptime_seconds":      int((datetime.utcnow() - STARTED_AT_UTC).total_seconds()),
        "wake_ready":          True,
        "session_configured":  bool(SESSION_STRING and SESSION_STRING.strip()),
        "thread_pool_workers": THREAD_POOL._max_workers,
        "timeouts": {
            "primary_bot":  TIMEOUT_PRIMARY,
            "backup_bot":   TIMEOUT_BACKUP,
            "silence_secs": SILENCE_SECS,
        },
    })


@app.route("/status", methods=["GET"])
def status():
    bots_info = {}
    for bid in ALL_BOTS:
        blocked = _bot_blocked(bid)
        ts      = _bot_fail_tracker.get(bid)
        bots_info[bid] = {
            "blocked":   blocked,
            "last_fail": ts.isoformat() if ts else None,
            "block_h":   BOT_BLOCK_HOURS if blocked else 0,
        }
    return jsonify({
        "status":         "ready",
        "version":        APP_VERSION,
        "uptime_seconds": int((datetime.utcnow() - STARTED_AT_UTC).total_seconds()),
        "session_ok":     bool(SESSION_STRING and SESSION_STRING.strip()),
        "credentials_ok": API_ID != 0 and bool(API_HASH),
        "bots":           bots_info,
        "in_flight":      len(_in_flight),
    })


@app.route("/debug", methods=["GET"])
def debug():
    return jsonify({
        "version":   APP_VERSION,
        "uptime_seconds": int((datetime.utcnow() - STARTED_AT_UTC).total_seconds()),
        "bot_primary":  {"id": BOT_PRIMARY,  "blocked": _bot_blocked(BOT_PRIMARY),  "timeout": TIMEOUT_PRIMARY},
        "bot_backup":   {"id": BOT_BACKUP,   "blocked": _bot_blocked(BOT_BACKUP),   "timeout": TIMEOUT_BACKUP},
        "silence_secs":        SILENCE_SECS,
        "block_hours":         BOT_BLOCK_HOURS,
        "thread_pool_workers": THREAD_POOL._max_workers,
        "requests_in_flight":  len(_in_flight),
    })


@app.route("/files/<path:filename>", methods=["GET"])
def serve_file(filename):
    return send_from_directory(DOWNLOAD_DIR, filename, as_attachment=True)


# ══════════════════════════════════════════════════════════════════
#  ENDPOINT PRINCIPAL
# ══════════════════════════════════════════════════════════════════
@app.route("/seeker", methods=["GET"])
def seeker():
    """
    Consulta datos completos de un ciudadano peruano por DNI.

    Parametro:  ?dni=XXXXXXXX  (8 digitos exactos)
    Ejemplo:    GET /seeker?dni=43652762

    Respuesta:
    {
      "status": "success",
      "total_mensajes": 9,
      "foto_url": "https://.../seeker_foto.jpg",
      "datos": {
        "dni": "43652762 - 0",
        "apellidos": "BAZAN ROSILLO",
        "nombres": "YAN CARLOS",
        "genero": "MASCULINO",
        "fecha_nacimiento": "13/08/1983",
        ...todos los campos que envíe el bot...
      },
      "direcciones": [...],
      "correos":     [...],
      "sueldos":     [...],
      "telefonos":   [...],
      "otros":       [...]
    }
    """
    raw_dni = request.args.get("dni", "").strip()

    dni, err = validate_dni(raw_dni)
    if err:
        return jsonify({
            "status":  "error",
            "message": err,
            "ejemplo": "GET /seeker?dni=43652762",
        }), 400

    command     = f"/seeker {dni}"
    consulta_id = f"seeker_{dni}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

    print(f"\n{'═'*60}")
    print(f"🔎 CONSULTA /seeker v5.0")
    print(f"   DNI     : {dni}")
    print(f"   Comando : {command}")
    print(f"   ID      : {consulta_id}")
    print(f"{'═'*60}")

    try:
        result = run_seeker(command, consulta_id)

        if result.get("status") == "error":
            msg  = result.get("message", "")
            code = 500
            if "incorrecto" in msg.lower() or "formato" in msg.lower():
                code = 400
            elif "no se encontraron" in msg.lower():
                code = 404
            elif "bloqueado" in msg.lower() or "no respondio" in msg.lower():
                code = 503
            return jsonify(result), code

        return jsonify(result), 200

    except FutureTimeoutError:
        return jsonify({
            "status":  "error",
            "message": "Timeout total excedido. El bot no respondio a tiempo.",
        }), 504
    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "status":  "error",
            "message": f"Error interno: {str(e)}",
        }), 500


# ══════════════════════════════════════════════════════════════════
#  PUNTO DE ENTRADA
# ══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("═" * 62)
    print("🚀  /seeker API Gateway  v5.0 — Full Capture")
    print("═" * 62)
    print(f"   Puerto              : {PORT}")
    print(f"   Bot principal       : {BOT_PRIMARY}")
    print(f"   Bot de respaldo     : {BOT_BACKUP}")
    print(f"   Timeout principal   : {TIMEOUT_PRIMARY}s")
    print(f"   Timeout respaldo    : {TIMEOUT_BACKUP}s")
    print(f"   Silencio dinamico   : {SILENCE_SECS}s")
    print(f"   Bloqueo bot         : {BOT_BLOCK_HOURS}h")
    print(f"   Hilos de proceso    : {THREAD_POOL._max_workers}")
    print()
    print("  FIXES vs v4.0:")
    print("  ✓ Bug #1: _detect_section() en raw_text (no en texto limpio)")
    print("  ✓ Bug #2: Caption de foto procesado como datos adicionales")
    print("  ✓ Log detallado con preview de cada mensaje recibido")
    print("  ✓ Sección 'otro' sin etiqueta extra innecesaria")
    print("═" * 62)
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True, use_reloader=False)
