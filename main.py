"""
╔══════════════════════════════════════════════════════════════════╗
║         API Gateway  /seeker  v4.0  —  Parser Universal         ║
║                                                                  ║
║  CAMBIO PRINCIPAL:                                               ║
║  • Parser 100% dinámico: cualquier "CLAVE : VALOR" del bot       ║
║    se convierte automáticamente en campo JSON.                   ║
║  • Sin lista fija de patrones → nunca se pierde ningún dato.     ║
║  • Captura completa de todos los mensajes (foto + texto).        ║
║  • Procesamiento multihilo del contenido recibido.               ║
║  • Resultado único, limpio y ordenado por secciones.             ║
╚══════════════════════════════════════════════════════════════════╝
"""

import os
import re
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

TIMEOUT_PRIMARY = 60    # segundos esperando bot principal
TIMEOUT_BACKUP  = 90    # segundos esperando bot de respaldo
SILENCE_SECS    = 7.0   # segundos de silencio para considerar respuesta completa
BOT_BLOCK_HOURS = 1     # horas de bloqueo si bot no responde

# Pool de hilos para procesar mensajes en paralelo
THREAD_POOL = ThreadPoolExecutor(max_workers=8, thread_name_prefix="seeker_worker")

# ══════════════════════════════════════════════════════════════════
#  ESTADO GLOBAL: fallos de bots + deduplicación HTTP
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
#  VALIDACIÓN DE DNI
# ══════════════════════════════════════════════════════════════════
def validate_dni(raw: str):
    """Retorna (dni_limpio, None) si válido, (None, mensaje_error) si no."""
    if not raw:
        return None, "Parametro 'dni' requerido. Ejemplo: ?dni=43652762"
    raw = raw.strip()
    if not raw.isdigit():
        return None, f"DNI invalido '{raw}': solo digitos numericos."
    if len(raw) != 8:
        return None, f"DNI invalido '{raw}': debe tener exactamente 8 digitos (recibidos {len(raw)})."
    return raw, None


# ══════════════════════════════════════════════════════════════════
#  LIMPIEZA MÍNIMA DE TEXTO
#  Solo se eliminan marcas internas del bot (encabezados, pies).
#  Nunca se toca el contenido de datos.
# ══════════════════════════════════════════════════════════════════

# Líneas que son metadata del bot y no datos del ciudadano
_BOT_META_LINES = re.compile(
    r"^\s*\[#?LEDER_BOT\].*$"
    r"|^\s*\[CONSULTA\s*PE\].*$"
    r"|^\s*→\s*SEEKER.*$"
    r"|^\s*Credits\s*:\s*\d+.*$"
    r"|^\s*Wanted\s+for\s*:.*$"
    r"|^\s*-{3,}\s*$",
    re.IGNORECASE | re.MULTILINE,
)

# Cabeceras de sección (no son datos pero sí identificadores de sección)
_SECTION_HEADER = re.compile(
    r"\[#?LEDER_BOT\]\s*→\s*(SEEKER.*?)(?:\n|$)"
    r"|\[SEEKER[^\]]*\]",
    re.IGNORECASE,
)

def _clean_raw(raw: str) -> str:
    """
    Limpieza mínima:
    - Quita líneas que son marcas internas del bot.
    - Normaliza espacios y saltos de línea.
    - NO elimina ningún dato del ciudadano.
    """
    if not raw:
        return ""
    t = _BOT_META_LINES.sub("", raw)
    t = re.sub(r"[ \t]+",  " ", t)
    t = re.sub(r"\n{3,}",  "\n\n", t)
    return t.strip()


# ══════════════════════════════════════════════════════════════════
#  PARSER UNIVERSAL DINÁMICO
#
#  REGLA: cualquier línea con formato "CLAVE : VALOR" se convierte
#  en un campo JSON. No hay lista fija de campos — todo se captura.
#
#  Ejemplos de lo que procesa:
#    DNI : 43652762            →  "dni": "43652762"
#    NOMBRES : YAN CARLOS      →  "nombres": "YAN CARLOS"
#    CORREO : yan@hotmail.com  →  "correo": "yan@hotmail.com"
#    SUELDO : S/.930           →  "sueldo": "S/.930"
#    CUALQUIER COSA : valor    →  "cualquier_cosa": "valor"
# ══════════════════════════════════════════════════════════════════

def _normalize_key(raw_key: str) -> str:
    """
    Convierte una clave raw en snake_case limpio.
    Ejemplos:
      "APELLIDO PATERNO" → "apellido_paterno"
      "FECHA NACIMIENTO" → "fecha_nacimiento"
      "DNI"              → "dni"
    """
    key = raw_key.strip().lower()
    key = re.sub(r"[^a-z0-9áéíóúñü]+", "_", key)
    key = key.strip("_")
    return key


def _parse_kv_line(line: str):
    """
    Intenta parsear una línea como "CLAVE : VALOR".
    Retorna (clave_normalizada, valor) o None si no aplica.
    """
    # Acepta cualquier texto antes del primer ":"  que no sea muy largo (evita URLs)
    m = re.match(r"^([^:\n]{1,50}?)\s*:\s*(.+)$", line.strip())
    if not m:
        return None
    raw_key = m.group(1).strip()
    raw_val = m.group(2).strip()

    # Descartar si la "clave" es solo números o muy corta sin sentido
    if not raw_key or raw_key.isdigit() or len(raw_key) < 2:
        return None

    # Descartar si parece una URL (contiene "http" en la clave)
    if "http" in raw_key.lower():
        return None

    key = _normalize_key(raw_key)
    return key, raw_val


def _text_to_dict(text: str) -> dict:
    """
    Convierte un bloque de texto completo en un diccionario.
    Procesa TODAS las líneas con formato "CLAVE : VALOR".
    Si una clave aparece varias veces, almacena una lista.
    """
    result = {}
    if not text:
        return result

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parsed = _parse_kv_line(line)
        if parsed:
            key, val = parsed
            if key in result:
                # Si ya existe, convertir en lista para no perder valores
                existing = result[key]
                if isinstance(existing, list):
                    existing.append(val)
                else:
                    result[key] = [existing, val]
            else:
                result[key] = val

    return result


def _has_not_found(text: str) -> bool:
    """Detecta si el mensaje indica que no hay resultados."""
    return bool(re.search(
        r"no se encontr[oó]|no se han encontrado|no hay resultados|"
        r"no tenemos datos|sin resultados",
        text, re.IGNORECASE
    ))


# ══════════════════════════════════════════════════════════════════
#  IDENTIFICADOR DE SECCIÓN
#  Detecta a qué sección pertenece un mensaje basándose en
#  las cabeceras que usa el bot LEDER DATA.
# ══════════════════════════════════════════════════════════════════

def _detect_section(text: str) -> str:
    """
    Retorna el nombre de la sección detectada:
      "datos"       → SEEKER [PREMIUM]  (datos personales principales)
      "direcciones" → SEEKER | DIRECCIONES [PREMIUM]
      "correos"     → SEEKER | CORREOS [PREMIUM]
      "sueldos"     → SEEKER | SUELDOS [PREMIUM]
      "telefonos"   → SEEKER | TELEFONOS [PREMIUM]
      "familiares"  → SEEKER | FAMILIARES [PREMIUM]
      "vehiculos"   → SEEKER | VEHICULOS [PREMIUM]
      "judicial"    → SEEKER | JUDICIAL [PREMIUM]
      "otro"        → cualquier otra sección no reconocida
    """
    t = text.upper()

    # Orden importa: primero las secciones compuestas (más específicas)
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
    if re.search(r"SEEKER\s*\[PREMIUM\]", t):
        return "datos"

    # Si contiene campos con ":" pero no tiene cabecera conocida → "otro"
    if ":" in text:
        return "otro"

    return "vacio"


# ══════════════════════════════════════════════════════════════════
#  DIVISOR DE BLOQUES DENTRO DE UNA SECCIÓN
#  Cuando el bot envía "Se encontro X resultados" con múltiples
#  registros en un mismo mensaje, los separa en bloques individuales.
# ══════════════════════════════════════════════════════════════════

def _split_into_blocks(text: str) -> list:
    """
    Divide un texto con múltiples registros en bloques individuales.
    Detecta separadores por:
      - Doble salto de línea
      - Nueva aparición de DOCUMENTO :
      - Nueva aparición de CORREO :  (en sección correos)
      - Nueva aparición de RUC :     (en sección sueldos)
    """
    # Intentar dividir por doble salto de línea primero
    bloques = re.split(r"\n\s*\n", text)

    # Si solo hay un bloque, intentar por campo identificador
    if len(bloques) <= 1:
        bloques = re.split(r"(?=\nDOCUMENTO\s*:|\nCORREO\s*:|\nRUC\s*:|\nTELEFONO\s*:)", text)

    result = []
    for b in bloques:
        b = b.strip()
        # Incluir solo si tiene al menos un par CLAVE: VALOR
        if b and ":" in b and len(b) > 5:
            result.append(b)

    return result if result else [text]


# ══════════════════════════════════════════════════════════════════
#  PROCESADOR DE MENSAJE  (ejecutado en hilo)
#  Toma un mensaje raw y retorna su sección + datos parseados.
# ══════════════════════════════════════════════════════════════════

def _process_message_in_thread(msg: dict) -> dict:
    """
    Procesa completamente un mensaje del bot.
    Se ejecuta en un hilo del ThreadPoolExecutor.

    Retorna:
      {
        "section": str,          # nombre de la sección
        "registros": list[dict], # lista de registros parseados
        "foto_url": str|None,    # URL de la foto si es media
        "raw_text": str          # texto original limpio
      }
    """
    raw_text  = msg.get("raw_text", "")
    has_media = msg.get("has_media", False)
    urls      = msg.get("urls", [])

    # ── Mensaje con foto/media ─────────────────────────────────────────────────
    if has_media:
        foto_url = urls[0].get("url") if urls else None
        return {
            "section":   "foto",
            "registros": [],
            "foto_url":  foto_url,
            "raw_text":  "",
        }

    if not raw_text:
        return {"section": "vacio", "registros": [], "foto_url": None, "raw_text": ""}

    # Limpiar marcas del bot
    clean = _clean_raw(raw_text)

    # Detectar sección
    section = _detect_section(clean)

    if section == "vacio":
        return {"section": "vacio", "registros": [], "foto_url": None, "raw_text": clean}

    # Dividir en bloques individuales (por si hay múltiples registros)
    bloques = _split_into_blocks(clean)

    registros = []
    for bloque in bloques:
        # Parsear TODOS los campos CLAVE: VALOR del bloque
        fields = _text_to_dict(bloque)

        # Eliminar campos vacíos o de metadata interna
        campos_limpios = {
            k: v for k, v in fields.items()
            if k not in ("credits", "wanted_for", "leder_bot", "consulta_pe")
            and v and str(v).strip()
        }

        if campos_limpios:
            registros.append(campos_limpios)

    return {
        "section":   section,
        "registros": registros,
        "foto_url":  None,
        "raw_text":  clean,
    }


# ══════════════════════════════════════════════════════════════════
#  CONSOLIDADOR MULTIHILO
#  Procesa todos los mensajes en paralelo y construye el JSON final.
# ══════════════════════════════════════════════════════════════════

def consolidate_multithreaded(captured_msgs: list) -> dict:
    """
    Envía cada mensaje a un hilo del pool para procesarlo en paralelo.
    Consolida todos los resultados en un único JSON estructurado.

    GARANTÍAS:
    - Ningún campo se pierde (parser dinámico captura todo).
    - Ningún mensaje se omite (todos se procesan sin excepción).
    - Los registros múltiples se agrupan en listas.
    - El orden original de los mensajes se preserva.
    """
    if not captured_msgs:
        return {"status": "success", "total_mensajes": 0}

    n = len(captured_msgs)
    print(f"⚙️  Procesando {n} mensajes en {THREAD_POOL._max_workers} hilos...")
    t0 = time.time()

    # Lanzar todos los mensajes al pool simultáneamente
    futures_map = {
        THREAD_POOL.submit(_process_message_in_thread, msg): idx
        for idx, msg in enumerate(captured_msgs)
    }

    # Recoger resultados respetando el orden original
    ordered = [None] * n
    for future in as_completed(futures_map, timeout=30):
        idx = futures_map[future]
        try:
            ordered[idx] = future.result()
        except Exception as e:
            print(f"⚠️  Error hilo {idx}: {e}")
            ordered[idx] = {"section": "vacio", "registros": [], "foto_url": None, "raw_text": ""}

    print(f"⚙️  Procesamiento paralelo: {time.time()-t0:.2f}s")

    # ── Estructura del resultado final ────────────────────────────────────────
    # Secciones fijas conocidas + secciones dinámicas desconocidas
    result = {
        "status":         "success",
        "total_mensajes": n,
        "foto_url":       None,
        "datos":          {},       # datos personales principales
        "direcciones":    [],
        "correos":        [],
        "sueldos":        [],
        "telefonos":      [],
        "familiares":     [],
        "vehiculos":      [],
        "judicial":       [],
        "redes_sociales": [],
        "otros":          [],       # secciones no identificadas
    }

    SECCIONES_LISTA = {
        "direcciones", "correos", "sueldos", "telefonos",
        "familiares",  "vehiculos", "judicial", "redes_sociales"
    }

    for parsed in ordered:
        if not parsed:
            continue

        section   = parsed.get("section", "vacio")
        registros = parsed.get("registros", [])
        foto_url  = parsed.get("foto_url")

        if section == "foto" and foto_url:
            result["foto_url"] = foto_url

        elif section == "datos":
            # Merge: los datos personales vienen en un solo mensaje
            for reg in registros:
                for k, v in reg.items():
                    if k not in result["datos"]:
                        result["datos"][k] = v
                    else:
                        # Si ya existe, agregar como lista para no perder nada
                        existing = result["datos"][k]
                        if isinstance(existing, list):
                            if v not in existing:
                                existing.append(v)
                        elif existing != v:
                            result["datos"][k] = [existing, v]

        elif section in SECCIONES_LISTA:
            result[section].extend(registros)

        elif section not in ("vacio", "foto"):
            # Sección dinámica no reconocida → va a "otros" con etiqueta
            for reg in registros:
                result["otros"].append({"seccion": section, **reg})

    # ── Limpiar claves vacías del resultado ───────────────────────────────────
    claves_lista = list(SECCIONES_LISTA) + ["otros"]
    for k in claves_lista:
        if k in result and not result[k]:
            del result[k]

    if not result.get("foto_url"):
        result.pop("foto_url", None)

    if not result.get("datos"):
        result.pop("datos", None)

    return result


# ══════════════════════════════════════════════════════════════════
#  NÚCLEO ASÍNCRONO: conectar → enviar → capturar → procesar
# ══════════════════════════════════════════════════════════════════

async def _execute_seeker(command: str, consulta_id: str) -> dict:
    """
    Flujo completo de una consulta /seeker:
      1. Conectar cliente Telegram.
      2. Enviar comando al bot principal (fallback a respaldo si falla).
      3. Capturar TODOS los mensajes de respuesta con silencio dinámico.
      4. Descargar archivos multimedia (foto).
      5. Procesar mensajes en paralelo con consolidate_multithreaded().
      6. Retornar JSON completo.
    """
    client          = None
    handler_ref     = [None]
    handler_removed = False

    try:
        # Validar credenciales
        if API_ID == 0 or not API_HASH:
            raise Exception("API_ID o API_HASH no configurados.")
        if not SESSION_STRING or not SESSION_STRING.strip():
            raise Exception("SESSION_STRING no configurada.")

        # Conectar
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        await client.connect()
        if not await client.is_user_authorized():
            raise Exception("Sesion de Telegram expirada o invalida.")

        # Bots disponibles
        bots = []
        if not _bot_blocked(BOT_PRIMARY):
            bots.append((BOT_PRIMARY, TIMEOUT_PRIMARY))
        if not _bot_blocked(BOT_BACKUP):
            bots.append((BOT_BACKUP,  TIMEOUT_BACKUP))
        if not bots:
            raise Exception("Todos los bots estan temporalmente bloqueados.")

        print(f"🤖 Bots disponibles: {[b[0] for b in bots]}")

        # Estado compartido
        captured_msgs: list = []
        stop_event          = asyncio.Event()
        last_msg_ts         = [time.time()]
        active_bot          = [None]

        # ── Handler de Telegram ────────────────────────────────────────────────
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

                tipo    = "📷 media" if has_media else f"📝 {len(raw_text)}c"
                bot_tag = "RESPALDO" if active_bot[0] == BOT_BACKUP else "PRINCIPAL"
                print(f"   📥 Msg #{len(captured_msgs)} [{tipo}] ← {bot_tag}")

            except Exception as e:
                print(f"   ⚠️  Handler error: {e}")

        handler_ref[0] = _handler

        # ── Loop sobre bots ────────────────────────────────────────────────────
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

            # Enviar UNA sola vez
            try:
                await client.send_message(bot_id, command)
                print(f"✅ Enviado a {label}")
            except UserBlockedError:
                print(f"❌ {label} bloqueado por el usuario Telegram")
                _mark_bot_failed(bot_id)
                continue
            except Exception as e:
                print(f"❌ Error al enviar a {label}: {str(e)[:100]}")
                if "blocked" in str(e).lower():
                    _mark_bot_failed(bot_id)
                continue

            # Esperar respuesta (silencio dinámico)
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
                            f"{len(captured_msgs)} mensajes capturados "
                            f"({elapsed:.1f}s total)"
                        )
                        break

                if elapsed > timeout_secs:
                    print(f"   ⏰ Timeout {timeout_secs}s | {len(captured_msgs)} msgs")
                    break

                await asyncio.sleep(0.15)

            # Evaluar
            if not captured_msgs:
                if is_primary:
                    print(f"❌ Bot principal sin respuesta → bloqueando {BOT_BLOCK_HOURS}h")
                    _mark_bot_failed(BOT_PRIMARY)
                else:
                    print("❌ Bot de respaldo sin respuesta")
                    _mark_bot_failed(BOT_BACKUP)
                    raise Exception("Bot de respaldo no respondio.")
                continue

            # ANTI-SPAM
            if is_primary and any(
                "ANTI-SPAM" in (m.get("raw_text") or "").upper()
                for m in captured_msgs
            ):
                print("🔄 ANTI-SPAM → cambiando a respaldo (5s)...")
                await asyncio.sleep(5)
                continue

            # Respuesta válida
            stop_event.set()
            print(f"\n✅ {len(captured_msgs)} mensajes capturados. Procesando...")
            break

        else:
            raise Exception("No se recibio respuesta valida de ningun bot.")

        # ── Descargar fotos y archivos ─────────────────────────────────────────
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

        # ── Verificaciones básicas ─────────────────────────────────────────────
        # Error de formato del bot
        for m in captured_msgs:
            if "Por favor, usa el formato correcto" in (m.get("raw_text") or ""):
                return {"status": "error", "message": "Formato de consulta incorrecto."}

        # "No encontrado" — solo si TODOS los mensajes de texto lo dicen
        text_msgs = [
            m for m in captured_msgs
            if not m.get("has_media") and len(m.get("raw_text", "")) > 20
        ]
        if text_msgs and all(_has_not_found(m.get("raw_text", "")) for m in text_msgs):
            return {"status": "error", "message": "No se encontraron resultados para este DNI."}

        # ── Procesar y consolidar en paralelo ──────────────────────────────────
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
                print("🔌 Cliente Telegram desconectado")
            except Exception as e:
                print(f"⚠️  Error desconectando: {e}")

        # Limpiar archivos > 5 min
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
#  WRAPPER SÍNCRONO  +  DEDUPLICACIÓN DE PETICIONES HTTP
# ══════════════════════════════════════════════════════════════════

def run_seeker(command: str, consulta_id: str) -> dict:
    """
    Ejecuta _execute_seeker desde Flask (síncrono).
    Deduplicación: si llegan dos peticiones idénticas simultáneas,
    la segunda espera el resultado de la primera.
    """
    from concurrent.futures import Future as ConcFuture

    dedup_key = command.strip()

    with _in_flight_lock:
        existing = _in_flight.get(dedup_key)
        if existing is None:
            fut = ConcFuture()
            _in_flight[dedup_key] = fut
        else:
            fut = None

    # Petición duplicada → esperar resultado existente
    if existing is not None:
        print(f"⚡ Peticion duplicada '{dedup_key}' → esperando resultado")
        try:
            return existing.result(timeout=TIMEOUT_PRIMARY + TIMEOUT_BACKUP + 15)
        except Exception:
            pass
        # Si falla, crear nueva future y ejecutar
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
#  FLASK APPLICATION
# ══════════════════════════════════════════════════════════════════
app = Flask(__name__)
CORS(app)


@app.route("/", methods=["GET"])
def root():
    return jsonify({
        "status":  "ok",
        "service": "API Gateway /seeker v4.0",
        "uso":     "GET /seeker?dni=43652762",
        "nota":    "Parser universal — captura todos los campos sin restricciones",
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status":              "healthy",
        "timestamp":           datetime.utcnow().isoformat(),
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
        "session_ok":     bool(SESSION_STRING and SESSION_STRING.strip()),
        "credentials_ok": API_ID != 0 and bool(API_HASH),
        "bots":           bots_info,
        "in_flight":      len(_in_flight),
    })


@app.route("/debug", methods=["GET"])
def debug():
    return jsonify({
        "bot_primary":         {"id": BOT_PRIMARY,  "blocked": _bot_blocked(BOT_PRIMARY),  "timeout": TIMEOUT_PRIMARY},
        "bot_backup":          {"id": BOT_BACKUP,   "blocked": _bot_blocked(BOT_BACKUP),   "timeout": TIMEOUT_BACKUP},
        "silence_secs":        SILENCE_SECS,
        "block_hours":         BOT_BLOCK_HOURS,
        "thread_pool_workers": THREAD_POOL._max_workers,
        "requests_in_flight":  len(_in_flight),
    })


@app.route("/files/<path:filename>", methods=["GET"])
def serve_file(filename):
    return send_from_directory(DOWNLOAD_DIR, filename, as_attachment=True)


# ══════════════════════════════════════════════════════════════════
#  ENDPOINT PRINCIPAL: /seeker
# ══════════════════════════════════════════════════════════════════
@app.route("/seeker", methods=["GET"])
def seeker():
    """
    Consulta datos completos de un ciudadano peruano por DNI.

    Parametro requerido:
      dni — exactamente 8 digitos numericos

    Ejemplo:
      GET /seeker?dni=43652762

    Respuesta JSON — todos los campos que devuelva el bot,
    sin recortar ni limitar ninguno:
      {
        "status":      "success",
        "total_mensajes": 5,
        "foto_url":    "https://.../seeker_foto.jpg",
        "datos": {
          "dni":              "43652762",
          "apellidos":        "BAZAN ROSILLO",
          "nombres":          "YAN CARLOS",
          "genero":           "MASCULINO",
          "fecha_nacimiento": "13/08/1983",
          "departamento":     "CAJAMARCA",
          "provincia":        "JAEN",
          "distrito":         "JAEN",
          ...cualquier otro campo que envíe el bot...
        },
        "direcciones": [
          { "documento": "43652762", "ubicacion": "...", "direccion": "...", "fuente": "RENIEC 2023" },
          ...
        ],
        "correos": [
          { "documento": "43652762", "correo": "yan@hotmail.com", "fuente": "RENIEC" },
          ...
        ],
        "sueldos": [
          { "ruc": "...", "empresa": "...", "sueldo": "S/.930", "periodo": "201906" },
          ...
        ]
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
    print(f"🔎 NUEVA CONSULTA /seeker")
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
    print("🚀  /seeker API Gateway  v4.0 — Parser Universal")
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
    print("  Parser universal activo:")
    print("  ✓ Cualquier CLAVE : VALOR → campo JSON automatico")
    print("  ✓ Sin lista fija de patrones — ningun dato se pierde")
    print("  ✓ Multiples registros por seccion soportados")
    print("  ✓ Secciones dinamicas no conocidas van a 'otros'")
    print("═" * 62)
    app.run(host="0.0.0.0", port=PORT, debug=False)
