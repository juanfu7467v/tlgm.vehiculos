import os
import re
import asyncio
import time
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# ─────────────────────────────────────────────
# CONFIGURACIÓN DE ENTORNO
# ─────────────────────────────────────────────
API_ID         = int(os.getenv("API_ID", "0"))
API_HASH       = os.getenv("API_HASH", "")
PUBLIC_URL     = os.getenv("PUBLIC_URL", "").rstrip("/")
SESSION_STRING = os.getenv("SESSION_STRING", None)
PORT           = int(os.getenv("PORT", 8080))

# ─────────────────────────────────────────────
# BOT ÚNICO
# ─────────────────────────────────────────────
AZURA_BOT   = "@AzuraSearchServices_bot"
TIMEOUT_SEC = 30   # Espera máxima: 30 segundos

# ─────────────────────────────────────────────
# DIRECTORIO DE DESCARGAS
# ─────────────────────────────────────────────
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


# ─────────────────────────────────────────────
# PARSER UNIVERSAL  (clave: valor → dict)
# ─────────────────────────────────────────────
def universal_parser(raw_text: str) -> dict:
    """
    Detecta líneas con formato 'Clave: Valor' y las convierte en dict.
    """
    if not raw_text:
        return {}
    parsed  = {}
    pattern = r'^([^:\n]+):\s*(.+?)(?=\n[^:\n]+:|$)'
    for m in re.finditer(pattern, raw_text, re.MULTILINE | re.DOTALL):
        key_raw   = m.group(1).strip()
        value_raw = m.group(2).strip()
        if not key_raw or not value_raw:
            continue
        key = re.sub(r'\s+', '_', key_raw.lower())
        key = re.sub(r'[^\w_]', '', key)
        val = re.sub(r'\s+', ' ', value_raw).strip()
        parsed[key] = val
    return parsed


# ─────────────────────────────────────────────
# CONSTRUIR RESPUESTA FINAL
# ─────────────────────────────────────────────
def build_response(messages: list, file_urls: list) -> dict:
    """
    Consolida mensajes recibidos, aplica parser universal y devuelve JSON.
    """
    combined = "\n".join(
        (m.get("message") or "").strip()
        for m in messages
        if (m.get("message") or "").strip()
    ).strip()

    if not combined:
        return {"status": "error", "message": "Sin respuesta del bot."}

    parsed   = universal_parser(combined)
    response = {"status": "success"}

    if parsed:
        response["data"]        = parsed
        response["raw_message"] = combined
    else:
        response["message"] = combined

    if file_urls:
        response.setdefault("data", {})
        response["data"]["urls"] = file_urls

    return response


# ─────────────────────────────────────────────
# ENVÍO A AZURA  (envío único, sin reintentos)
# ─────────────────────────────────────────────
async def send_azura_command(command: str) -> dict:
    """
    Envía el comando UNA SOLA VEZ a @AzuraSearchServices_bot.
    Espera hasta TIMEOUT_SEC segundos.
    Procesa la respuesta en cuanto llega.
    """
    if API_ID == 0 or not API_HASH or not SESSION_STRING:
        return {"status": "error", "message": "Credenciales de Telegram no configuradas."}

    client = None
    try:
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        await client.connect()

        if not await client.is_user_authorized():
            return {"status": "error", "message": "Sesión de Telegram no autorizada."}

        bot_entity        = await client.get_entity(AZURA_BOT)
        messages_received = []
        stop_event        = asyncio.Event()

        @client.on(events.NewMessage(incoming=True))
        async def _handler(event):
            if event.sender_id != bot_entity.id:
                return
            raw = (event.raw_text or "").strip()
            messages_received.append({
                "message":       raw,
                "event_message": event.message,
            })
            if raw:
                stop_event.set()    # Detener espera al recibir respuesta

        # ── ENVÍO ÚNICO ──────────────────────────────────────────────
        print(f"[Azura] Enviando: {command}")
        await client.send_message(AZURA_BOT, command)

        # ── ESPERA (máx. TIMEOUT_SEC segundos) ───────────────────────
        start = time.time()
        while not stop_event.is_set() and (time.time() - start) < TIMEOUT_SEC:
            await asyncio.sleep(0.3)

        client.remove_event_handler(_handler)

        if not messages_received:
            return {
                "status":  "error",
                "message": f"Sin respuesta del bot tras {TIMEOUT_SEC} segundos.",
            }

        # ── DESCARGAR ARCHIVOS ADJUNTOS ───────────────────────────────
        file_urls = []
        for msg_obj in messages_received:
            ev_msg = msg_obj.get("event_message")
            if ev_msg and getattr(ev_msg, "media", None):
                try:
                    ext   = ".pdf" if "pdf" in str(ev_msg.media).lower() else ".jpg"
                    fname = f"{int(time.time())}_{ev_msg.id}{ext}"
                    path  = await client.download_media(
                        ev_msg, file=os.path.join(DOWNLOAD_DIR, fname)
                    )
                    if path:
                        file_urls.append({
                            "url":  f"{PUBLIC_URL}/files/{fname}",
                            "type": "document",
                        })
                except Exception as dl_err:
                    print(f"[Azura] Error al descargar archivo: {dl_err}")

        return build_response(messages_received, file_urls)

    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        if client:
            await client.disconnect()


def run_command(command: str) -> dict:
    """Ejecuta send_azura_command en event loop propio (thread-safe para Flask)."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(send_azura_command(command))
    finally:
        loop.close()


# ─────────────────────────────────────────────
# VALIDACIONES
# ─────────────────────────────────────────────
def validate_placa(placa: str) -> str | None:
    """Placa de 6 o 7 caracteres alfanuméricos."""
    if not placa:
        return "Parámetro 'placa' requerido."
    if not re.match(r'^[A-Za-z0-9]{6,7}$', placa.strip()):
        return "La placa debe tener 6 o 7 caracteres alfanuméricos. Ej: ABC123"
    return None

def validate_placa_6(placa: str) -> str | None:
    """Placa exactamente de 6 caracteres."""
    if not placa:
        return "Parámetro 'placa' requerido."
    if not re.match(r'^[A-Za-z0-9]{6}$', placa.strip()):
        return "La placa debe tener exactamente 6 caracteres. Ej: ABC123"
    return None

def validate_dni(dni: str) -> str | None:
    """DNI de 8 dígitos."""
    if not dni:
        return "Parámetro 'dni' requerido."
    if not re.match(r'^\d{8}$', dni.strip()):
        return "El DNI debe tener exactamente 8 dígitos. Ej: 45454545"
    return None

def validate_doc(doc: str) -> str | None:
    """DNI (8 dígitos) o Carnet de Extranjería (9 dígitos)."""
    if not doc:
        return "Parámetro 'dni' o 'carnet' requerido."
    if not re.match(r'^\d{8,9}$', doc.strip()):
        return "Ingrese DNI (8 dígitos) o Carnet de Extranjería (9 dígitos)."
    return None


# ─────────────────────────────────────────────
# APP FLASK
# ─────────────────────────────────────────────
app = Flask(__name__)
CORS(app)


@app.route("/files/<path:filename>")
def serve_file(filename):
    return send_from_directory(DOWNLOAD_DIR, filename)


@app.route("/health")
def health():
    return jsonify({"status": "healthy", "bot": AZURA_BOT})


@app.route("/status")
def status():
    return jsonify({
        "status":  "online",
        "bot":     AZURA_BOT,
        "timeout": f"{TIMEOUT_SEC}s",
        "endpoints": [
            "/placav?placa=ABC123",
            "/citv?placa=ABC123",
            "/revisiones?placa=ABC123",
            "/rvt?placa=ABC123",
            "/placab?placa=ABC123",
            "/licencia?dni=45454545",
            "/mtc?dni=45454545",
            "/mtc?carnet=002436285",
            "/papeletas?placa=ABC123",
            "/soat?placa=ABC123",
            "/placar?placa=ABC123",
        ],
    })


# ══════════════════════════════════════════════════════════════
# COMANDOS — Rutas individuales con validación propia
# ══════════════════════════════════════════════════════════════

# 1) TARJETA DE IDENTIFICACIÓN VEHICULAR FÍSICA
#    GET /placav?placa=ABC123
@app.route("/placav")
def route_placav():
    placa = request.args.get("placa", "")
    err   = validate_placa(placa)
    if err:
        return jsonify({"status": "error", "message": err}), 400
    return jsonify(run_command(f"/placav {placa.strip().upper()}"))


# 2) CONSULTA REVISIÓN TÉCNICA EN PDF
#    GET /citv?placa=ABC123
#    GET /revisiones?placa=ABC123
#    GET /rvt?placa=ABC123
@app.route("/citv")
def route_citv():
    placa = request.args.get("placa", "")
    err   = validate_placa_6(placa)
    if err:
        return jsonify({"status": "error", "message": err}), 400
    return jsonify(run_command(f"/citv {placa.strip().upper()}"))


@app.route("/revisiones")
def route_revisiones():
    placa = request.args.get("placa", "")
    err   = validate_placa_6(placa)
    if err:
        return jsonify({"status": "error", "message": err}), 400
    return jsonify(run_command(f"/revisiones {placa.strip().upper()}"))


@app.route("/rvt")
def route_rvt():
    placa = request.args.get("placa", "")
    err   = validate_placa_6(placa)
    if err:
        return jsonify({"status": "error", "message": err}), 400
    return jsonify(run_command(f"/rvt {placa.strip().upper()}"))


# 3) BOLETA INFORMATIVA VEHICULAR — SUNARP
#    GET /placab?placa=ABC123
@app.route("/placab")
def route_placab():
    placa = request.args.get("placa", "")
    err   = validate_placa(placa)
    if err:
        return jsonify({"status": "error", "message": err}), 400
    return jsonify(run_command(f"/placab {placa.strip().upper()}"))


# 4) LICENCIA DE CONDUCIR ELECTRÓNICA PDF
#    GET /licencia?dni=45454545
@app.route("/licencia")
def route_licencia():
    dni = request.args.get("dni", "")
    err = validate_dni(dni)
    if err:
        return jsonify({"status": "error", "message": err}), 400
    return jsonify(run_command(f"/licencia {dni.strip()}"))


# 5) CERTIFICADO REPORTE MTC — PAPELETAS PDF
#    GET /mtc?dni=45454545
#    GET /mtc?carnet=002436285
@app.route("/mtc")
def route_mtc():
    doc = request.args.get("dni") or request.args.get("carnet", "")
    err = validate_doc(doc)
    if err:
        return jsonify({"status": "error", "message": err}), 400
    return jsonify(run_command(f"/mtc {doc.strip()}"))


# 6) PAPELETAS SAT
#    GET /papeletas?placa=ABC123
@app.route("/papeletas")
def route_papeletas():
    placa = request.args.get("placa", "")
    err   = validate_placa(placa)
    if err:
        return jsonify({"status": "error", "message": err}), 400
    return jsonify(run_command(f"/papeletas {placa.strip().upper()}"))


# 7) SOAT DIGITAL EN PDF
#    GET /soat?placa=ABC123
@app.route("/soat")
def route_soat():
    placa = request.args.get("placa", "")
    err   = validate_placa(placa)
    if err:
        return jsonify({"status": "error", "message": err}), 400
    return jsonify(run_command(f"/soat {placa.strip().upper()}"))


# 8) REPORTE REGISTRO PLACA
#    GET /placar?placa=ABC123
@app.route("/placar")
def route_placar():
    placa = request.args.get("placa", "")
    err   = validate_placa(placa)
    if err:
        return jsonify({"status": "error", "message": err}), 400
    return jsonify(run_command(f"/placar {placa.strip().upper()}"))


# ─────────────────────────────────────────────
# PUNTO DE ENTRADA
# ─────────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
