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

# ══════════════════════════════════════════════════════════════
#  VARIABLES DE ENTORNO
# ══════════════════════════════════════════════════════════════
API_ID         = int(os.getenv("API_ID", "0"))
API_HASH       = os.getenv("API_HASH", "")
PUBLIC_URL     = os.getenv("PUBLIC_URL", "").rstrip("/")
SESSION_STRING = os.getenv("SESSION_STRING", None)
PORT           = int(os.getenv("PORT", 8080))

# ══════════════════════════════════════════════════════════════
#  CONFIGURACIÓN INTERNA
# ══════════════════════════════════════════════════════════════
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

BOT_PRIMARY  = "@LEDERDATA_OFC_BOT"
BOT_BACKUP   = "@lederdata_publico_bot"
ALL_BOTS     = [BOT_PRIMARY, BOT_BACKUP]

# Tiempo máximo esperando respuesta del bot (segundos)
TIMEOUT_PRIMARY = 60
TIMEOUT_BACKUP  = 90

# Segundos de silencio tras el último mensaje para considerar la
# respuesta completa. /seeker envía ~5 mensajes con pausas de 3-4s.
SILENCE_SECS = 6.0

# Horas de bloqueo de un bot cuando no responde
BOT_BLOCK_HOURS = 1

# Pool de hilos para procesar mensajes en paralelo
# (limpieza, extracción de campos, descarga de media)
THREAD_POOL = ThreadPoolExecutor(max_workers=8, thread_name_prefix="seeker_worker")

# ══════════════════════════════════════════════════════════════
#  TRACKEO DE FALLOS  +  DEDUPLICACIÓN DE PETICIONES HTTP
# ══════════════════════════════════════════════════════════════
_bot_fail_tracker: dict = {}
_in_flight: dict        = {}          # dedup: comando → Future
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


# ══════════════════════════════════════════════════════════════
#  VALIDACIÓN DE DNI
# ══════════════════════════════════════════════════════════════
def validate_dni(raw: str):
    """Retorna (dni, None) si válido, o (None, msg_error) si no."""
    if not raw:
        return None, "Parametro 'dni' requerido. Ejemplo: ?dni=43652762"
    raw = raw.strip()
    if not raw.isdigit():
        return None, f"DNI invalido '{raw}': solo digitos numericos."
    if len(raw) != 8:
        return None, f"DNI invalido '{raw}': debe tener 8 digitos (recibidos {len(raw)})."
    return raw, None


# ══════════════════════════════════════════════════════════════
#  LIMPIEZA DE TEXTO  (mínima, no destructiva)
# ══════════════════════════════════════════════════════════════
def _clean(raw: str) -> str:
    """
    Elimina solo encabezados y marcas del bot.
    NO toca el contenido de datos (correos, sueldos, etc.).
    """
    if not raw:
        return ""
    t = raw
    t = re.sub(r"\[#?LEDER_BOT\]",            "", t, flags=re.IGNORECASE)
    t = re.sub(r"\[CONSULTA\s*PE\]",           "", t, flags=re.IGNORECASE)
    t = re.sub(r"→\s*SEEKER[^\n]*",            "", t, flags=re.IGNORECASE)
    # Quitar Credits/Wanted for solo al FINAL del texto
    t = re.sub(r"\nCredits\s*:\s*\d+[^\n]*$",  "", t, flags=re.IGNORECASE | re.MULTILINE)
    t = re.sub(r"\nWanted for\s*:[^\n]*$",      "", t, flags=re.IGNORECASE | re.MULTILINE)
    t = re.sub(r"-{3,}",                        "", t)
    t = re.sub(r"[ \t]+",                       " ", t)
    t = re.sub(r"\n{3,}",                       "\n\n", t)
    return t.strip()


# ══════════════════════════════════════════════════════════════
#  EXTRACCIÓN DE CAMPOS  (ejecutada en hilo)
# ══════════════════════════════════════════════════════════════
# Todos los patrones que usa el bot LEDER DATA en /seeker
_FIELD_PATTERNS = {
    "dni":              r"DNI\s*:\s*([\d\s\-]+)",
    "apellidos":        r"APELLIDOS\s*:\s*(.+?)(?:\n|$)",
    "apellido_paterno": r"APELLIDO\s+PATERNO\s*:\s*(.+?)(?:\n|$)",
    "apellido_materno": r"APELLIDO\s+MATERNO\s*:\s*(.+?)(?:\n|$)",
    "nombres":          r"NOMBRES?\s*:\s*(.+?)(?:\n|$)",
    "genero":           r"GENERO\s*:\s*(.+?)(?:\n|$)",
    "fecha_nacimiento": r"FECHA\s+NACIMIENTO\s*:\s*(.+?)(?:\n|$)",
    "departamento":     r"DEPARTAMENTO\s*:\s*(.+?)(?:\n|$)",
    "provincia":        r"PROVINCIA\s*:\s*(.+?)(?:\n|$)",
    "distrito":         r"DISTRITO\s*:\s*(.+?)(?:\n|$)",
    "documento":        r"DOCUMENTO\s*:\s*(\d+)",
    "ubicacion":        r"UBICACION\s*:\s*(.+?)(?:\n|$)",
    "direccion":        r"DIRECCION\s*:\s*(.+?)(?:\n|$)",
    "fuente":           r"FUENTE\s*:\s*(.+?)(?:\n|$)",
    "correo":           r"CORREO\s*:\s*(.+?)(?:\n|$)",
    "fecha":            r"FECHA\s*:\s*(.+?)(?:\n|$)",
    "ruc":              r"RUC\s*:\s*(\d+)",
    "empresa":          r"EMPRESA\s*:\s*(.+?)(?:\n|$)",
    "situacion":        r"SITUACION\s*:\s*(.+?)(?:\n|$)",
    "sueldo":           r"SUELDO\s*:\s*(.+?)(?:\n|$)",
    "periodo":          r"PERIODO\s*:\s*(.+?)(?:\n|$)",
}

_NOT_FOUND_RE = re.compile(
    r"no se encontr[oó]|no se han encontrado|no hay resultados|"
    r"no tenemos datos|sin resultados",
    re.IGNORECASE,
)


def _extract_fields(text: str) -> dict:
    """Extrae todos los campos CLAVE: VALOR del texto. Corre en hilo."""
    fields = {}
    if not text:
        return fields
    for key, pattern in _FIELD_PATTERNS.items():
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            if val:
                fields[key] = val
    if _NOT_FOUND_RE.search(text):
        fields["not_found"] = True
    return fields


def _process_text_message(raw_text: str) -> dict:
    """
    Limpia el texto y extrae los campos.
    Esta función se ejecuta en un hilo del ThreadPoolExecutor.
    """
    clean = _clean(raw_text)
    fields = _extract_fields(clean)
    return {"clean": clean, "fields": fields}


# ══════════════════════════════════════════════════════════════
#  DIVISOR DE BLOQUES  (para secciones multi-registro)
# ══════════════════════════════════════════════════════════════
def _split_blocks(text: str) -> list:
    """Divide un texto con múltiples registros en bloques individuales."""
    raw_blocks = re.split(r"\n\s*\n|(?=\nDOCUMENTO\s*:)", text)
    result = []
    for b in raw_blocks:
        b = b.strip()
        if b and ":" in b and len(b) > 10:
            result.append(b)
    return result or [text]


# ══════════════════════════════════════════════════════════════
#  PARSER MULTI-SECCIÓN  (ejecutado en hilo)
# ══════════════════════════════════════════════════════════════
def _parse_section(msg: dict) -> dict:
    """
    Determina a qué sección pertenece un mensaje y extrae su contenido.
    Corre en un hilo independiente del ThreadPoolExecutor.

    Retorna un dict con:
      {
        "section":  "datos" | "direcciones" | "correos" | "sueldos" | "foto" | "otro",
        "payload":  <datos extraídos>
      }
    """
    raw_text  = msg.get("raw_text", "")
    clean     = msg.get("message", "")       # ya limpio si fue preprocesado
    has_media = msg.get("has_media", False)
    urls      = msg.get("urls", [])
    texto     = clean or raw_text

    # ── Foto ──────────────────────────────────────────────────────────────────
    if has_media:
        url = urls[0].get("url") if urls else None
        return {"section": "foto", "payload": url}

    if not texto:
        return {"section": "vacio", "payload": None}

    # ── Detectar tipo de sección ───────────────────────────────────────────────
    es_dir  = bool(re.search(r"SEEKER\s*\|\s*DIRECCIONES|DIRECCIONES\s*\[PREMIUM\]", texto, re.I))
    es_cor  = bool(re.search(r"SEEKER\s*\|\s*CORREOS|CORREOS\s*\[PREMIUM\]",         texto, re.I))
    es_sue  = bool(re.search(r"SEEKER\s*\|\s*SUELDOS|SUELDOS\s*\[PREMIUM\]",         texto, re.I))
    es_dat  = bool(re.search(r"SEEKER\s*\[PREMIUM\]", texto, re.I) and not es_dir and not es_cor and not es_sue)

    if es_dat:
        return {"section": "datos", "payload": _extract_fields(texto)}

    if es_dir:
        bloques = _split_blocks(texto)
        return {"section": "direcciones", "payload": [_extract_fields(b) for b in bloques if _extract_fields(b)]}

    if es_cor:
        bloques = _split_blocks(texto)
        return {"section": "correos", "payload": [_extract_fields(b) for b in bloques if _extract_fields(b)]}

    if es_sue:
        bloques = _split_blocks(texto)
        return {"section": "sueldos", "payload": [_extract_fields(b) for b in bloques if _extract_fields(b)]}

    # ── Sección no identificada → extracción genérica ─────────────────────────
    campos = _extract_fields(texto)
    if campos and campos != {"not_found": True}:
        return {"section": "otro", "payload": {"texto": texto, "campos": campos}}

    return {"section": "vacio", "payload": None}


# ══════════════════════════════════════════════════════════════
#  CONSOLIDADOR MULTIHILO
# ══════════════════════════════════════════════════════════════
def consolidate_messages_multithreaded(all_messages: list) -> dict:
    """
    Procesa TODOS los mensajes recibidos en paralelo usando el ThreadPoolExecutor.

    Cada mensaje se analiza en su propio hilo:
      - Limpieza de texto
      - Extracción de campos
      - Clasificación de sección

    Luego los resultados se consolidan en un único JSON ordenado.
    """
    result = {
        "status":           "success",
        "datos_personales": {},
        "direcciones":      [],
        "correos":          [],
        "sueldos":          [],
        "otros":            [],
        "foto_url":         None,
        "total_mensajes":   len(all_messages),
    }

    if not all_messages:
        return result

    print(f"⚙️  Procesando {len(all_messages)} mensajes en paralelo ({THREAD_POOL._max_workers} hilos)...")
    t_start = time.time()

    # Enviar cada mensaje al pool y guardar el future junto con su índice
    futures_map = {}
    for idx, msg in enumerate(all_messages):
        future = THREAD_POOL.submit(_parse_section, msg)
        futures_map[future] = idx

    # Recolectar resultados manteniendo orden de llegada
    ordered = [None] * len(all_messages)
    for future in as_completed(futures_map, timeout=30):
        idx = futures_map[future]
        try:
            ordered[idx] = future.result()
        except Exception as e:
            print(f"⚠️  Error en hilo {idx}: {e}")
            ordered[idx] = {"section": "vacio", "payload": None}

    print(f"⚙️  Procesamiento paralelo completado en {time.time()-t_start:.2f}s")

    # Consolidar resultados en orden
    for parsed in ordered:
        if parsed is None:
            continue
        sec     = parsed["section"]
        payload = parsed["payload"]

        if sec == "foto" and payload:
            result["foto_url"] = payload

        elif sec == "datos" and payload:
            result["datos_personales"].update(payload)

        elif sec == "direcciones" and payload:
            result["direcciones"].extend(payload)

        elif sec == "correos" and payload:
            result["correos"].extend(payload)

        elif sec == "sueldos" and payload:
            result["sueldos"].extend(payload)

        elif sec == "otro" and payload:
            result["otros"].append(payload)

    # Eliminar claves vacías para respuesta limpia
    for key in ["direcciones", "correos", "sueldos", "otros", "foto_url"]:
        if not result[key]:
            del result[key]

    return result


# ══════════════════════════════════════════════════════════════
#  NÚCLEO ASÍNCRONO:  enviar comando y capturar respuestas
# ══════════════════════════════════════════════════════════════
async def _execute_seeker(command: str, consulta_id: str) -> dict:
    """
    1. Conecta cliente Telegram.
    2. Envía /seeker <dni> al bot principal (o de respaldo si está bloqueado).
    3. Captura TODOS los mensajes de respuesta (texto + media).
    4. Descarga archivos multimedia.
    5. Llama a consolidate_messages_multithreaded() para parsear en paralelo.
    6. Retorna el JSON consolidado.
    """
    client          = None
    handler_ref     = [None]
    handler_removed = False

    try:
        # ── Validar credenciales ───────────────────────────────────────────────
        if API_ID == 0 or not API_HASH:
            raise Exception("API_ID o API_HASH no configurados.")
        if not SESSION_STRING or not SESSION_STRING.strip():
            raise Exception("SESSION_STRING no configurada.")

        # ── Conectar cliente Telegram ──────────────────────────────────────────
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        await client.connect()
        if not await client.is_user_authorized():
            raise Exception("Sesion de Telegram expirada o invalida.")

        # ── Seleccionar bots disponibles ───────────────────────────────────────
        bots = []
        if not _bot_blocked(BOT_PRIMARY):
            bots.append((BOT_PRIMARY, TIMEOUT_PRIMARY))
        if not _bot_blocked(BOT_BACKUP):
            bots.append((BOT_BACKUP,  TIMEOUT_BACKUP))

        if not bots:
            raise Exception("Todos los bots estan temporalmente bloqueados.")

        print(f"🤖 Bots en cola: {[b[0] for b in bots]}")

        # ── Estado compartido entre handler y loop ─────────────────────────────
        captured_msgs: list = []          # mensajes capturados (thread-safe por GIL en append)
        stop_event          = asyncio.Event()
        last_msg_ts         = [time.time()]
        active_bot          = [None]

        # ── Handler de Telegram (async, corre en el loop de asyncio) ──────────
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

                # Pre-procesar texto en hilo para no bloquear el loop de asyncio
                if raw_text:
                    loop = asyncio.get_event_loop()
                    processed = await loop.run_in_executor(
                        THREAD_POOL,
                        _process_text_message,
                        raw_text
                    )
                    clean  = processed["clean"]
                    fields = processed["fields"]
                else:
                    clean  = ""
                    fields = {}

                msg_obj = {
                    "raw_text":      raw_text,
                    "message":       clean,
                    "fields":        fields,
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

        # ══════════════════════════════════════════════════════════════════════
        #  LOOP SOBRE BOTS
        # ══════════════════════════════════════════════════════════════════════
        for bot_id, timeout_secs in bots:
            is_primary = (bot_id == BOT_PRIMARY)
            label      = "BOT PRINCIPAL" if is_primary else "BOT RESPALDO"

            # Resetear estado para este intento
            captured_msgs.clear()
            stop_event.clear()
            last_msg_ts[0] = time.time()
            active_bot[0]  = bot_id

            print(f"\n{'─'*55}")
            print(f"🎯 {label}: {bot_id}")
            print(f"   Comando : {command}")
            print(f"   Timeout : {timeout_secs}s  |  Silencio: {SILENCE_SECS}s")
            print(f"{'─'*55}")

            # ── Enviar comando (UNA SOLA VEZ) ──────────────────────────────────
            try:
                await client.send_message(bot_id, command)
                print(f"✅ Enviado a {label}")
            except UserBlockedError:
                print(f"❌ {label} bloqueado por el usuario")
                _mark_bot_failed(bot_id)
                continue
            except Exception as e:
                print(f"❌ Error enviando a {label}: {str(e)[:100]}")
                if "blocked" in str(e).lower():
                    _mark_bot_failed(bot_id)
                continue

            # ── Esperar respuesta completa ──────────────────────────────────────
            t0              = time.time()
            first_msg_time  = [None]

            while True:
                elapsed = time.time() - t0
                silence = time.time() - last_msg_ts[0]

                if captured_msgs:
                    if first_msg_time[0] is None:
                        first_msg_time[0] = time.time()
                        print(f"   ⏱️  Primer mensaje en {first_msg_time[0]-t0:.1f}s")

                    if silence >= SILENCE_SECS:
                        print(f"   ✅ Silencio {silence:.1f}s → captura completa ({len(captured_msgs)} msgs, {elapsed:.1f}s total)")
                        break

                if elapsed > timeout_secs:
                    print(f"   ⏰ Timeout {timeout_secs}s | {len(captured_msgs)} msgs recibidos")
                    break

                await asyncio.sleep(0.15)

            # ── Evaluar resultado ───────────────────────────────────────────────
            if not captured_msgs:
                if is_primary:
                    print(f"❌ Bot principal sin respuesta → bloqueando {BOT_BLOCK_HOURS}h")
                    _mark_bot_failed(BOT_PRIMARY)
                else:
                    print("❌ Bot de respaldo sin respuesta")
                    _mark_bot_failed(BOT_BACKUP)
                    raise Exception("Bot de respaldo no respondio a tiempo.")
                continue

            # Detectar ANTI-SPAM
            if is_primary and any("ANTI-SPAM" in m.get("message", "").upper() for m in captured_msgs):
                print("🔄 ANTI-SPAM detectado → cambiando a bot de respaldo (5s pausa)...")
                await asyncio.sleep(5)
                continue

            # ── Respuesta válida → procesar ────────────────────────────────────
            stop_event.set()
            print(f"\n✅ {len(captured_msgs)} mensajes capturados. Iniciando procesamiento...")
            break   # salir del loop de bots

        else:
            # El for terminó sin un break → ningún bot respondió
            raise Exception("No se recibio respuesta valida de ningun bot.")

        # ════════════════════════════════════════════════════════════════════════
        #  DESCARGAR MULTIMEDIA  (secuencial porque Telethon no es thread-safe)
        # ════════════════════════════════════════════════════════════════════════
        print("📥 Descargando archivos multimedia...")
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

                ts   = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
                fname = f"seeker_{ts}_{idx}{file_ext}"
                fpath = os.path.join(DOWNLOAD_DIR, fname)

                if not client.is_connected():
                    await client.connect()

                saved = await client.download_media(event_msg, file=fpath)
                if saved and os.path.exists(saved):
                    url = f"{PUBLIC_URL}/files/{os.path.basename(saved)}"
                    msg["urls"]      = [{"url": url, "type": content_type}]
                    msg["has_media"] = True
                    print(f"   ✅ {os.path.basename(saved)}")

            except Exception as e:
                print(f"   ⚠️  Error descargando msg {idx}: {e}")

        # Remover handler antes de desconectar
        try:
            client.remove_event_handler(handler_ref[0])
            handler_removed = True
        except Exception:
            pass

        # ════════════════════════════════════════════════════════════════════════
        #  VERIFICACIONES PREVIAS AL PARSE
        # ════════════════════════════════════════════════════════════════════════
        # Error de formato
        for m in captured_msgs:
            if "Por favor, usa el formato correcto" in (m.get("message") or m.get("raw_text", "")):
                return {"status": "error", "message": "Formato de consulta incorrecto."}

        # "No encontrado" — solo si TODOS los mensajes de texto lo dicen
        text_msgs = [m for m in captured_msgs if not m.get("has_media") and len(m.get("raw_text", "")) > 30]
        if text_msgs and all(m.get("fields", {}).get("not_found") for m in text_msgs):
            return {"status": "error", "message": "No se encontraron resultados para este DNI."}

        # ════════════════════════════════════════════════════════════════════════
        #  CONSOLIDAR RESULTADO EN PARALELO (multihilo)
        # ════════════════════════════════════════════════════════════════════════
        return consolidate_messages_multithreaded(captured_msgs)

    except Exception as e:
        print(f"❌ Error en _execute_seeker: {e}")
        traceback.print_exc()
        return {"status": "error", "message": f"Error al procesar: {str(e)}"}

    finally:
        # Garantizar desconexión limpia
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

        # Limpiar archivos temporales > 5 min
        try:
            now = time.time()
            removed = sum(
                1 for fn in os.listdir(DOWNLOAD_DIR)
                if os.path.isfile(fp := os.path.join(DOWNLOAD_DIR, fn))
                and now - os.path.getmtime(fp) > 300
                and not os.remove(fp)   # os.remove retorna None → truthy en sum
            )
            if removed:
                print(f"🧹 {removed} archivos temporales eliminados")
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════
#  WRAPPER SÍNCRONO PARA FLASK
#  Incluye deduplicación: peticiones idénticas concurrentes
#  comparten el mismo resultado en lugar de duplicar el envío.
# ══════════════════════════════════════════════════════════════
def run_seeker(command: str, consulta_id: str) -> dict:
    """Ejecuta _execute_seeker en un event loop propio y retorna el resultado."""
    from concurrent.futures import Future as ConcFuture

    dedup_key = command.strip()

    with _in_flight_lock:
        if dedup_key in _in_flight:
            existing = _in_flight[dedup_key]
        else:
            existing = None
            fut = ConcFuture()
            _in_flight[dedup_key] = fut

    # Si ya hay una petición idéntica en vuelo, esperar su resultado
    if existing is not None:
        print(f"⚡ Peticion duplicada '{dedup_key}' — esperando resultado existente")
        try:
            return existing.result(timeout=TIMEOUT_PRIMARY + TIMEOUT_BACKUP + 15)
        except Exception:
            pass  # Si falla, ejecutar normalmente
        # Crear nueva future para este intento
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


# ══════════════════════════════════════════════════════════════
#  FLASK APP
# ══════════════════════════════════════════════════════════════
app = Flask(__name__)
CORS(app)


# ── Raíz ──────────────────────────────────────────────────────
@app.route("/", methods=["GET"])
def root():
    return jsonify({
        "status":  "ok",
        "service": "API Gateway /seeker",
        "version": "3.0 — solo seeker, multihilo",
        "uso":     "GET /seeker?dni=43652762",
    })


# ── Health ────────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status":             "healthy",
        "timestamp":          datetime.utcnow().isoformat(),
        "session_configured": bool(SESSION_STRING and SESSION_STRING.strip()),
        "thread_pool_workers": THREAD_POOL._max_workers,
        "timeouts": {
            "primary":  TIMEOUT_PRIMARY,
            "backup":   TIMEOUT_BACKUP,
            "silence":  SILENCE_SECS,
        },
    })


# ── Status de bots ────────────────────────────────────────────
@app.route("/status", methods=["GET"])
def status():
    bots = {}
    for bid in ALL_BOTS:
        blocked = _bot_blocked(bid)
        ts      = _bot_fail_tracker.get(bid)
        bots[bid] = {
            "blocked":    blocked,
            "last_fail":  ts.isoformat() if ts else None,
            "block_h":    BOT_BLOCK_HOURS if blocked else 0,
        }
    return jsonify({
        "status":         "ready",
        "session_ok":     bool(SESSION_STRING and SESSION_STRING.strip()),
        "credentials_ok": API_ID != 0 and bool(API_HASH),
        "bots":           bots,
    })


# ── Debug ─────────────────────────────────────────────────────
@app.route("/debug", methods=["GET"])
def debug():
    return jsonify({
        "bot_primary": {
            "id":      BOT_PRIMARY,
            "blocked": _bot_blocked(BOT_PRIMARY),
            "timeout": TIMEOUT_PRIMARY,
        },
        "bot_backup": {
            "id":      BOT_BACKUP,
            "blocked": _bot_blocked(BOT_BACKUP),
            "timeout": TIMEOUT_BACKUP,
        },
        "silence_secs":        SILENCE_SECS,
        "block_hours":         BOT_BLOCK_HOURS,
        "thread_pool_workers": THREAD_POOL._max_workers,
        "requests_in_flight":  len(_in_flight),
    })


# ── Servir archivos descargados ───────────────────────────────
@app.route("/files/<path:filename>", methods=["GET"])
def serve_file(filename):
    return send_from_directory(DOWNLOAD_DIR, filename, as_attachment=True)


# ══════════════════════════════════════════════════════════════
#  ENDPOINT PRINCIPAL: /seeker
# ══════════════════════════════════════════════════════════════
@app.route("/seeker", methods=["GET"])
def seeker():
    """
    Consulta datos completos de un ciudadano peruano por DNI.

    Parámetros GET:
      dni  — número de DNI, exactamente 8 dígitos numéricos.

    Uso:
      GET /seeker?dni=43652762

    Respuesta JSON (éxito):
      {
        "status":           "success",
        "datos_personales": { "nombres": "...", "apellidos": "...", ... },
        "direcciones":      [ { "ubicacion": "...", "fuente": "..." }, ... ],
        "correos":          [ { "correo": "...", "fuente": "..." }, ... ],
        "sueldos":          [ { "empresa": "...", "sueldo": "...", ... }, ... ],
        "foto_url":         "https://...",
        "total_mensajes":   5
      }
    """
    raw_dni = request.args.get("dni", "").strip()

    # Validar DNI
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
    print(f"🔎 CONSULTA /seeker")
    print(f"   DNI     : {dni}")
    print(f"   Comando : {command}")
    print(f"   ID      : {consulta_id}")
    print(f"{'═'*60}")

    try:
        result = run_seeker(command, consulta_id)

        if result.get("status") == "error":
            msg  = result.get("message", "")
            code = 500
            if "formato" in msg.lower() or "incorrecto" in msg.lower():
                code = 400
            elif "no se encontraron" in msg.lower() or "no hay resultados" in msg.lower():
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


# ══════════════════════════════════════════════════════════════
#  PUNTO DE ENTRADA
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("═" * 60)
    print("🚀  /seeker API Gateway  v3.0")
    print("═" * 60)
    print(f"   Puerto              : {PORT}")
    print(f"   Bot principal       : {BOT_PRIMARY}")
    print(f"   Bot de respaldo     : {BOT_BACKUP}")
    print(f"   Timeout principal   : {TIMEOUT_PRIMARY}s")
    print(f"   Timeout respaldo    : {TIMEOUT_BACKUP}s")
    print(f"   Silencio dinamico   : {SILENCE_SECS}s")
    print(f"   Bloqueo bot         : {BOT_BLOCK_HOURS}h")
    print(f"   Hilos de proceso    : {THREAD_POOL._max_workers}")
    print("═" * 60)
    app.run(host="0.0.0.0", port=PORT, debug=False)
