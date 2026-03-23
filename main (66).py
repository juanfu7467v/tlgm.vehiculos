import os
import re
import asyncio
import traceback
import time
import json
import mimetypes
from datetime import datetime, timezone, timedelta
from urllib.parse import unquote
import concurrent.futures
from concurrent.futures import TimeoutError as FutureTimeoutError
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto
from telethon.errors.rpcerrorlist import UserBlockedError

# =============================================================================
# CONFIGURACIÓN Y VARIABLES DE ENTORNO
# =============================================================================
API_ID          = int(os.getenv("API_ID", "0"))
API_HASH        = os.getenv("API_HASH", "")
PUBLIC_URL      = os.getenv("PUBLIC_URL", "").rstrip("/")
SESSION_STRING  = os.getenv("SESSION_STRING", None)
PORT            = int(os.getenv("PORT", 8080))

# =============================================================================
# CONFIGURACIÓN INTERNA
# =============================================================================
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Bots de Telegram
LEDERDATA_BOT_ID        = "@LEDERDATA_OFC_BOT"
LEDERDATA_BACKUP_BOT_ID = "@lederdata_publico_bot"
ALL_BOT_IDS             = [LEDERDATA_BOT_ID, LEDERDATA_BACKUP_BOT_ID]

# Timeouts (segundos)
TIMEOUT_PRIMARY  = 25   # Espera máxima al bot principal  (spec: 25s)
TIMEOUT_BACKUP   = 50   # Espera máxima al bot de respaldo
BOT_BLOCK_HOURS  = 3    # Horas de bloqueo si bot principal no responde

# =============================================================================
# TRACKEO DE FALLOS Y DEDUPLICACIÓN
# =============================================================================
bot_fail_tracker: dict = {}
_in_flight_commands: dict = {}


def is_bot_blocked(bot_id: str) -> bool:
    """Verifica si el bot está en período de bloqueo por fallos recientes."""
    last_fail = bot_fail_tracker.get(bot_id)
    if not last_fail:
        return False
    if last_fail > datetime.now() - timedelta(hours=BOT_BLOCK_HOURS):
        return True
    print(f"✅ Bot {bot_id} desbloqueado (transcurrieron {BOT_BLOCK_HOURS}h).")
    bot_fail_tracker.pop(bot_id, None)
    return False


def record_bot_failure(bot_id: str):
    """Registra el fallo del bot y activa el período de bloqueo."""
    print(f"🚨 Bot {bot_id} bloqueado por {BOT_BLOCK_HOURS}h.")
    bot_fail_tracker[bot_id] = datetime.now()


# =============================================================================
# VALIDACIÓN DEL COMANDO /seeker
# =============================================================================
def validate_seeker_dni(dni: str):
    """
    Valida que el DNI cumpla las reglas del comando /seeker:
      - Exactamente 8 dígitos
      - Solo números (sin letras ni símbolos)
    Retorna (dni_limpio, None) si es válido, o (None, mensaje_error) si no.
    """
    if not dni:
        return None, (
            "Parámetro 'dni' requerido. "
            "Formato: /seeker <dni_8_digitos>  |  Ejemplo: /seeker 43652762"
        )

    dni = dni.strip()

    if not dni.isdigit():
        return None, (
            f"❌ DNI inválido '{dni}': solo debe contener números (sin letras ni símbolos). "
            "Ejemplo correcto: /seeker 43652762"
        )

    if len(dni) != 8:
        return None, (
            f"❌ DNI inválido '{dni}': debe tener exactamente 8 dígitos "
            f"(se recibieron {len(dni)}). "
            "Ejemplo correcto: /seeker 43652762"
        )

    return dni, None


# =============================================================================
# LIMPIEZA Y EXTRACCIÓN DE DATOS
# =============================================================================
def clean_and_extract(raw_text: str):
    """Limpia el texto del bot y extrae campos estructurados."""
    if not raw_text:
        return {"text": "", "fields": {}}

    text = raw_text

    # Eliminar menciones de bots/cabeceras
    text = re.sub(r"\[#?LEDER_BOT\]",  "", text, flags=re.IGNORECASE)
    text = re.sub(r"\[CONSULTA PE\]",   "", text, flags=re.IGNORECASE)

    # Eliminar cabecera completa
    text = re.sub(
        r"^\[.*?\]\s*→\s*.*?\[.*?\](\r?\n){1,2}",
        "", text, flags=re.IGNORECASE | re.DOTALL
    )

    # Eliminar pie de página
    text = re.sub(
        r"((\r?\n){1,2}\[|Página\s*\d+\/\d+.*"
        r"|(\r?\n){1,2}Por favor, usa el formato correcto.*"
        r"|↞ Anterior|Siguiente ↠.*|Credits\s*:.+"
        r"|Wanted for\s*:.+|\s*@lederdata.*"
        r"|(\r?\n){1,2}\s*Marca\s*@lederdata.*"
        r"|(\r?\n){1,2}\s*Créditos\s*:\s*\d+)",
        "", text, flags=re.IGNORECASE | re.DOTALL
    )

    # Limpiar separadores y espacios
    text = re.sub(r"\-{3,}", "", text)
    text = re.sub(r"\s+", " ", text)
    text = text.strip()

    # Extracción dinámica de campos
    fields = {}
    patterns = {
        "dni":              r"DNI\s*:\s*(\d{8})",
        "ruc":              r"RUC\s*:\s*(\d{11})",
        "apellido_paterno": r"APELLIDO\s+PATERNO\s*:\s*(.*?)(?:\n|$)",
        "apellido_materno": r"APELLIDO\s+MATERNO\s*:\s*(.*?)(?:\n|$)",
        "nombres":          r"NOMBRES\s*:\s*(.*?)(?:\n|$)",
        "estado":           r"ESTADO\s*:\s*(.*?)(?:\n|$)",
        "fecha_nacimiento": r"(?:FECHA\s+DE\s+NACIMIENTO|F\.?NAC\.?)\s*:\s*(.*?)(?:\n|$)",
        "genero":           r"(?:GÉNERO|SEXO)\s*:\s*(.*?)(?:\n|$)",
        "direccion":        r"(?:DIRECCIÓN|DOMICILIO)\s*:\s*(.*?)(?:\n|$)",
        "ubigeo":           r"UBIGEO\s*:\s*(.*?)(?:\n|$)",
        "departamento":     r"DEPARTAMENTO\s*:\s*(.*?)(?:\n|$)",
        "provincia":        r"PROVINCIA\s*:\s*(.*?)(?:\n|$)",
        "distrito":         r"DISTRITO\s*:\s*(TODO\s+EL\s+DISTRITO|.*?)(?:\n|$)",
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            fields[key] = match.group(1).strip()
            text = re.sub(pattern, "", text, flags=re.IGNORECASE | re.DOTALL)

    # Foto/tipo biométrico
    photo_match = re.search(
        r"Foto\s*:\s*(rostro|huella|firma|adverso|reverso).*",
        text, re.IGNORECASE
    )
    if photo_match:
        fields["photo_type"] = photo_match.group(1).lower()

    # Detección de "no encontrado"
    not_found_pattern = (
        r"\[⚠️\]\s*(no se encontro información|no se han encontrado resultados"
        r"|no se encontró una|no hay resultados|no tenemos datos"
        r"|no se encontraron registros)"
    )
    if re.search(not_found_pattern, text, re.IGNORECASE | re.DOTALL):
        fields["not_found"] = True

    text = re.sub(r"\n\s*\n", "\n", text).strip()
    return {"text": text, "fields": fields}


# =============================================================================
# CONSTRUCCIÓN DEL JSON LIMPIO DESDE MÚLTIPLES MENSAJES
# =============================================================================
def build_seeker_json(all_received_messages: list) -> dict:
    """
    Une todos los mensajes recibidos en un único JSON limpio y ordenado.
    Las palabras antes de ':' se usan como claves del JSON cuando el texto
    llega desordenado.
    """
    combined_text_parts = []
    final_fields = {}
    urls_list = []

    for msg in all_received_messages:
        # Acumular texto limpio
        msg_text = msg.get("message", "").strip()
        if msg_text:
            combined_text_parts.append(msg_text)

        # Unificar fields (sin sobrescribir los que ya tienen valor)
        for key, value in msg.get("fields", {}).items():
            if key not in final_fields and value:
                final_fields[key] = value

        # Recopilar URLs de archivos
        for url_obj in msg.get("urls", []):
            urls_list.append({
                "type": url_obj.get("type", "document"),
                "url":  url_obj.get("url"),
            })

    combined_text = " | ".join(combined_text_parts)

    # Si no hay campos estructurados, intentar extraerlos del texto combinado
    # usando el patrón "CLAVE : VALOR"
    if not final_fields and combined_text:
        for part in re.split(r"[|\n]", combined_text):
            kv_match = re.match(r"^([^:]{1,40}?)\s*:\s*(.+)$", part.strip())
            if kv_match:
                raw_key   = kv_match.group(1).strip().lower()
                raw_value = kv_match.group(2).strip()
                clean_key = re.sub(r"[^a-z0-9_]", "_", raw_key).strip("_")
                if clean_key and raw_value:
                    final_fields[clean_key] = raw_value

    # Construir respuesta final
    response: dict = {}

    # Datos estructurados primero
    response.update(final_fields)

    # Texto completo combinado (para referencia)
    if combined_text:
        response["raw_text"] = combined_text

    # Metadatos
    response["total_messages"] = len(all_received_messages)
    response["total_files"]    = len(urls_list)

    if urls_list:
        response["urls"] = urls_list

    return response


# =============================================================================
# FUNCIÓN PRINCIPAL ASÍNCRONA — ENVÍO Y ESPERA DE RESPUESTA
# =============================================================================
async def send_telegram_command(command: str, consulta_id: str = None, endpoint_path: str = None):
    """
    Envía el comando UNA SOLA VEZ al bot apropiado y espera la respuesta.

    Flujo:
      1. Bot principal no bloqueado → enviar UNA vez, esperar 25s.
         - Respuesta válida  → procesar y retornar.
         - Anti-spam         → pasar directamente al bot de respaldo.
         - Sin respuesta     → bloquear principal 3h, pasar al bot de respaldo.
      2. Bot de respaldo     → enviar UNA vez, esperar hasta 50s.
      3. Si bot principal bloqueado → ir directamente al bot de respaldo.

    Nunca se envía el mismo comando más de una vez por bot.
    """
    client = None
    handler_removed = False

    try:
        # Verificar credenciales
        if API_ID == 0 or not API_HASH:
            raise Exception("API_ID o API_HASH no configurados.")
        if not SESSION_STRING or not SESSION_STRING.strip():
            raise Exception("SESSION_STRING no configurada.")

        # Conectar cliente
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        await client.connect()
        if not await client.is_user_authorized():
            raise Exception("Sesión de Telegram expirada o inválida.")

        # Generar consulta_id si no se proporciona
        if not consulta_id:
            consulta_id = f"seeker_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # Determinar bots disponibles en orden de prioridad
        primary_blocked = is_bot_blocked(LEDERDATA_BOT_ID)
        backup_blocked  = is_bot_blocked(LEDERDATA_BACKUP_BOT_ID)

        if primary_blocked and backup_blocked:
            raise Exception("Todos los bots están temporalmente bloqueados. Intenta en unos minutos.")

        # Lista ordenada: (bot_id, timeout_segundos)
        bots_to_try = []
        if not primary_blocked:
            bots_to_try.append((LEDERDATA_BOT_ID,        TIMEOUT_PRIMARY))
        if not backup_blocked:
            bots_to_try.append((LEDERDATA_BACKUP_BOT_ID, TIMEOUT_BACKUP))

        print(f"🤖 Bots disponibles: {[b[0] for b in bots_to_try]}")

        # Variables compartidas entre intentos
        all_received_messages: list = []
        all_files_data: list        = []
        stop_collecting             = asyncio.Event()
        last_message_time           = [time.time()]
        current_bot_id              = [None]

        # Handler temporal — captura mensajes entrantes del bot activo
        @client.on(events.NewMessage(incoming=True))
        async def temp_handler(event):
            if stop_collecting.is_set():
                return
            try:
                if not current_bot_id[0]:
                    return
                try:
                    entity = await client.get_entity(current_bot_id[0])
                    if event.sender_id != entity.id:
                        return
                except Exception:
                    return

                last_message_time[0] = time.time()
                raw_text = event.raw_text or ""
                cleaned  = clean_and_extract(raw_text)

                msg_obj = {
                    "chat_id":       getattr(event, "chat_id", None),
                    "from_id":       event.sender_id,
                    "date":          (
                        event.message.date.isoformat()
                        if getattr(event, "message", None)
                        else datetime.utcnow().isoformat()
                    ),
                    "message":       cleaned["text"],
                    "fields":        cleaned["fields"],
                    "urls":          [],
                    "bot_id":        entity.id,
                    "event_message": event.message,
                }
                all_received_messages.append(msg_obj)
                is_backup = current_bot_id[0] == LEDERDATA_BACKUP_BOT_ID
                print(
                    f"📥 Mensaje recibido "
                    f"({'respaldo' if is_backup else 'principal'}): "
                    f"{len(cleaned['text'])} chars"
                )
            except Exception as e:
                print(f"⚠️ Error en handler: {e}")

        # Iterar sobre bots — cada uno se intenta UNA SOLA VEZ
        for bot_id, timeout_to_use in bots_to_try:
            is_primary = (bot_id == LEDERDATA_BOT_ID)
            bot_label  = "bot principal" if is_primary else "bot de respaldo"

            # Resetear estado
            all_received_messages.clear()
            all_files_data.clear()
            stop_collecting.clear()
            last_message_time[0] = time.time()
            current_bot_id[0]    = bot_id

            print(f"\n🎯 INTENTANDO CON {bot_label.upper()}: {bot_id}")
            print(f"   Comando : {command}")
            print(f"   Timeout : {timeout_to_use}s")

            # Enviar comando UNA SOLA VEZ
            try:
                await client.send_message(bot_id, command)
                print(f"✅ Comando enviado al {bot_label}")
            except UserBlockedError:
                print(f"❌ {bot_label.capitalize()} bloqueado por usuario")
                record_bot_failure(bot_id)
                continue
            except Exception as e:
                print(f"❌ Error enviando a {bot_label}: {str(e)[:120]}")
                if "blocked" in str(e).lower():
                    record_bot_failure(bot_id)
                continue

            # Esperar respuesta con detección de silencio dinámico
            start_time      = time.time()
            first_msg_time  = [None]

            while True:
                elapsed_total    = time.time() - start_time
                silence_duration = time.time() - last_message_time[0]

                if all_received_messages:
                    if first_msg_time[0] is None:
                        first_msg_time[0] = time.time()

                    time_to_first     = first_msg_time[0] - start_time
                    silence_threshold = 2.0 if time_to_first < 3.0 else 4.0

                    if silence_duration > silence_threshold:
                        print(
                            f"✅ Respuesta completa "
                            f"({len(all_received_messages)} msg, "
                            f"silencio {silence_duration:.1f}s)"
                        )
                        break

                if elapsed_total > timeout_to_use:
                    break

                await asyncio.sleep(0.3)

            # Analizar resultado del intento
            if not all_received_messages:
                if is_primary:
                    print(
                        f"⏰ TIMEOUT {timeout_to_use}s: "
                        f"Bot principal sin respuesta → bloqueando {BOT_BLOCK_HOURS}h"
                    )
                    record_bot_failure(LEDERDATA_BOT_ID)
                else:
                    print(f"⏰ TIMEOUT {timeout_to_use}s: Bot de respaldo sin respuesta")
                    record_bot_failure(LEDERDATA_BACKUP_BOT_ID)
                    raise Exception("Bot de respaldo no respondió a tiempo.")
                continue  # Probar siguiente bot

            # Verificar ANTI-SPAM
            anti_spam = any(
                "⛔ ANTI-SPAM" in m.get("message", "") or "ANTI-SPAM" in m.get("message", "")
                for m in all_received_messages
            )
            if anti_spam and is_primary:
                print("🔄 ANTI-SPAM detectado en bot principal → usando respaldo")
                await asyncio.sleep(5)
                continue

            # ✅ Respuesta válida — procesar y retornar
            print(f"✅ {bot_label.capitalize()}: {len(all_received_messages)} mensajes recibidos")
            stop_collecting.set()
            return await process_bot_response(
                client, temp_handler, all_received_messages,
                all_files_data, handler_removed, consulta_id, command, endpoint_path
            )

        # Ningún bot respondió satisfactoriamente
        raise Exception("No se recibió respuesta válida de ningún bot disponible.")

    except Exception as e:
        return {
            "status":  "error",
            "message": f"Error al procesar comando: {str(e)}",
        }

    finally:
        if client:
            try:
                if not handler_removed:
                    try:
                        client.remove_event_handler(temp_handler)
                    except Exception:
                        pass
                await client.disconnect()
                print("🔌 Cliente desconectado")
            except Exception as e:
                print(f"⚠️ Error desconectando: {e}")

        # Limpiar archivos de más de 5 minutos
        try:
            now = time.time()
            removed = 0
            for fn in os.listdir(DOWNLOAD_DIR):
                fp = os.path.join(DOWNLOAD_DIR, fn)
                if os.path.isfile(fp) and now - os.path.getmtime(fp) > 300:
                    os.remove(fp)
                    removed += 1
            if removed:
                print(f"🧹 Limpiados {removed} archivos temporales")
        except Exception as e:
            print(f"⚠️ Error limpiando archivos: {e}")


# =============================================================================
# PROCESAMIENTO DE RESPUESTA DEL BOT
# =============================================================================
async def process_bot_response(
    client, temp_handler, all_received_messages, all_files_data,
    handler_removed, consulta_id, command, endpoint_path
):
    """Valida y construye la respuesta final a partir de los mensajes del bot."""
    try:
        # Verificar error de formato
        for msg in all_received_messages:
            if "Por favor, usa el formato correcto" in msg.get("message", ""):
                if client and not handler_removed:
                    client.remove_event_handler(temp_handler)
                return {
                    "status":  "error",
                    "message": "Formato de consulta incorrecto. Verifica los parámetros enviados.",
                }

        # Verificar "no encontrado"
        for msg in all_received_messages:
            if msg.get("fields", {}).get("not_found", False):
                if client and not handler_removed:
                    client.remove_event_handler(temp_handler)
                return {
                    "status":  "error",
                    "message": "No se encontraron resultados para dicha consulta.",
                }

        # Descargar archivos multimedia
        print("📥 Procesando archivos multimedia...")
        for idx, msg in enumerate(all_received_messages):
            try:
                event_msg = msg.get("event_message")
                if not event_msg or not getattr(event_msg, "media", None):
                    continue

                media_list = []
                if isinstance(event_msg.media, (MessageMediaDocument, MessageMediaPhoto)):
                    media_list.append(event_msg.media)

                for i, media in enumerate(media_list):
                    try:
                        file_ext     = ".file"
                        content_type = "application/octet-stream"

                        if hasattr(media, "document") and hasattr(media.document, "attributes"):
                            file_name = getattr(media.document, "file_name", "file")
                            file_ext  = os.path.splitext(file_name)[1] or ".file"
                            if "pdf" in file_name.lower() or file_ext == ".pdf":
                                content_type = "application/pdf"
                            elif file_ext.lower() in [".jpg", ".jpeg"]:
                                content_type = "image/jpeg"
                            elif file_ext.lower() == ".png":
                                content_type = "image/png"
                        elif isinstance(media, MessageMediaPhoto) or (
                            hasattr(media, "photo") and media.photo
                        ):
                            file_ext     = ".jpg"
                            content_type = "image/jpeg"

                        ts              = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
                        unique_filename = f"{ts}_{event_msg.id}_{i}{file_ext}"

                        if not client.is_connected():
                            await client.connect()

                        saved_path = await client.download_media(
                            event_msg,
                            file=os.path.join(DOWNLOAD_DIR, unique_filename),
                        )

                        if saved_path and os.path.exists(saved_path):
                            with open(saved_path, "rb") as f:
                                file_content = f.read()
                            all_files_data.append((unique_filename, file_content, content_type))
                            if "urls" not in msg:
                                msg["urls"] = []
                            msg["urls"].append({
                                "url":  f"{PUBLIC_URL}/files/{os.path.basename(saved_path)}",
                                "type": "document",
                            })
                            print(f"   ✅ Descargado: {unique_filename}")

                    except Exception as e:
                        print(f"❌ Error descargando archivo {i} del msg {idx}: {e}")

            except Exception as e:
                print(f"❌ Error procesando msg {idx}: {e}")

        print(f"📊 Total archivos descargados: {len(all_files_data)}")

        # Remover handler
        if client and not handler_removed:
            client.remove_event_handler(temp_handler)

        # Construir y retornar JSON limpio
        result = build_seeker_json(all_received_messages)
        result["status"] = "success"
        return result

    except Exception as e:
        print(f"❌ Error en process_bot_response: {e}")
        return {
            "status":  "error",
            "message": f"Error procesando respuesta: {str(e)}",
        }


# =============================================================================
# WRAPPER SÍNCRONO PARA FLASK (con deduplicación)
# =============================================================================
def run_telegram_command(command: str, consulta_id: str = None, endpoint_path: str = None):
    """
    Ejecuta la corrutina asíncrona desde el contexto síncrono de Flask.
    Incluye deduplicación: si el mismo comando ya está en vuelo, espera
    el resultado existente en lugar de enviar uno nuevo al bot.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        dedup_key = command.strip()

        if dedup_key in _in_flight_commands:
            print(f"⚡ Comando duplicado detectado '{dedup_key}' — esperando resultado existente")
            try:
                return _in_flight_commands[dedup_key].result(
                    timeout=TIMEOUT_PRIMARY + TIMEOUT_BACKUP + 10
                )
            except Exception:
                pass  # Si falla, ejecutar normalmente

        future = concurrent.futures.Future()
        _in_flight_commands[dedup_key] = future

        try:
            result = loop.run_until_complete(
                send_telegram_command(command, consulta_id, endpoint_path)
            )
            future.set_result(result)
            return result
        except Exception as e:
            future.set_exception(e)
            raise
        finally:
            _in_flight_commands.pop(dedup_key, None)
    finally:
        loop.close()


# =============================================================================
# APLICACIÓN FLASK
# =============================================================================
app = Flask(__name__)
CORS(app)


# --- Raíz ---
@app.route("/", methods=["GET"])
def root():
    return jsonify({
        "status":    "ok",
        "message":   "API Gateway /seeker activo.",
        "mode":      "serverless",
        "version":   "1.0 - Solo comando /seeker",
        "endpoints": {
            "/seeker": "GET ?dni=<8_digitos>  →  Datos personales del ciudadano",
        },
    })


# --- Health check ---
@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({
        "status":             "healthy",
        "mode":               "serverless",
        "timestamp":          datetime.utcnow().isoformat(),
        "session_configured": bool(SESSION_STRING and SESSION_STRING.strip()),
        "features": {
            "multiple_messages": True,
            "idle_timeout":      True,
            "clean_json":        True,
            "field_extraction":  True,
            "bot_failover":      True,
            "bot_blocking":      True,
            "deduplication":     True,
        },
    })


# --- Estado de los bots ---
@app.route("/status", methods=["GET"])
def status():
    bot_status = {}
    for bot_id in ALL_BOT_IDS:
        blocked   = is_bot_blocked(bot_id)
        last_fail = bot_fail_tracker.get(bot_id)
        bot_status[bot_id] = {
            "blocked":     blocked,
            "last_fail":   last_fail.isoformat() if last_fail else None,
            "block_hours": BOT_BLOCK_HOURS if blocked else 0,
        }
    return jsonify({
        "status":          "ready",
        "session_loaded":  bool(SESSION_STRING and SESSION_STRING.strip()),
        "credentials_ok":  API_ID != 0 and bool(API_HASH),
        "bot_status":      bot_status,
        "timeouts": {
            "primary_bot":  TIMEOUT_PRIMARY,
            "backup_bot":   TIMEOUT_BACKUP,
            "block_hours":  BOT_BLOCK_HOURS,
        },
    })


# --- Debug de bots ---
@app.route("/debug/bots", methods=["GET"])
def debug_bots():
    return jsonify({
        "primary_bot": {
            "id":        LEDERDATA_BOT_ID,
            "blocked":   is_bot_blocked(LEDERDATA_BOT_ID),
            "last_fail": bot_fail_tracker.get(LEDERDATA_BOT_ID, None),
            "timeout":   TIMEOUT_PRIMARY,
        },
        "backup_bot": {
            "id":        LEDERDATA_BACKUP_BOT_ID,
            "blocked":   is_bot_blocked(LEDERDATA_BACKUP_BOT_ID),
            "last_fail": bot_fail_tracker.get(LEDERDATA_BACKUP_BOT_ID, None),
            "timeout":   TIMEOUT_BACKUP,
        },
        "block_hours": BOT_BLOCK_HOURS,
    })


# --- Servir archivos descargados ---
@app.route("/files/<path:filename>", methods=["GET"])
def files(filename):
    return send_from_directory(DOWNLOAD_DIR, filename, as_attachment=True)


# =============================================================================
# ENDPOINT ÚNICO: /seeker
# =============================================================================
@app.route("/seeker", methods=["GET"])
def seeker():
    """
    Consulta datos personales de un ciudadano peruano mediante DNI.

    Parámetros GET:
      - dni  (requerido): número de DNI, exactamente 8 dígitos numéricos.

    Formato del comando enviado al bot:
      /seeker <dni>

    Ejemplos:
      ✅ /seeker?dni=43652762
      ❌ /seeker?dni=876172        (menos de 8 dígitos)
      ❌ /seeker?dni=4365276A      (contiene letras)
    """
    raw_dni = request.args.get("dni", "").strip()

    # Validar DNI
    dni, validation_error = validate_seeker_dni(raw_dni)
    if validation_error:
        return jsonify({
            "status":  "error",
            "message": validation_error,
            "formato": {
                "correcto":   "/seeker?dni=43652762",
                "incorrecto": "/seeker?dni=876172  (menos de 8 dígitos)",
                "reglas": [
                    "El DNI debe tener exactamente 8 dígitos.",
                    "El DNI solo debe contener números (sin letras ni símbolos).",
                ],
            },
        }), 400

    # Construir comando para el bot
    command     = f"/seeker {dni}"
    consulta_id = f"seeker_{dni}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    print(f"\n{'='*60}")
    print(f"🔎 /seeker  |  DNI: {dni}  |  ID: {consulta_id}")
    print(f"   Comando : {command}")
    print(f"{'='*60}")

    try:
        result = run_telegram_command(command, consulta_id, "/seeker")

        if result.get("status") == "error":
            status_code = 500
            msg = result.get("message", "")
            if "incorrecto" in msg.lower() or "formato" in msg.lower():
                status_code = 400
            elif "no se encontraron" in msg.lower() or "no hay resultados" in msg.lower():
                status_code = 404
            elif "bloqueado" in msg.lower() or "no respondió" in msg.lower():
                status_code = 503
            return jsonify(result), status_code

        return jsonify(result), 200

    except FutureTimeoutError:
        return jsonify({
            "status":  "error",
            "message": f"Timeout excedido ({TIMEOUT_PRIMARY + TIMEOUT_BACKUP}s). Intenta nuevamente.",
        }), 504
    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "status":  "error",
            "message": f"Error interno: {str(e)}",
        }), 500


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("🚀 API Gateway /seeker — Modo Serverless")
    print("=" * 60)
    print(f"   Puerto          : {PORT}")
    print(f"   Timeout principal: {TIMEOUT_PRIMARY}s")
    print(f"   Timeout respaldo : {TIMEOUT_BACKUP}s")
    print(f"   Bloqueo bot      : {BOT_BLOCK_HOURS}h")
    print("   Comandos activos : /seeker <dni_8_digitos>")
    print("=" * 60)
    app.run(host="0.0.0.0", port=PORT, debug=False)
