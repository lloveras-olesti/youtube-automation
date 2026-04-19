#!/usr/bin/env python3
"""
Autenticador YouTube OAuth2
============================
Genera data/youtube_token.json usando las credenciales OAuth existentes
del proyecto canal-reli-automation.

#   python app/utils/auth_youtube.py
#
# Requisito previo único (ya debería estar hecho):
  - http://localhost:8080/ añadida a las URIs de redirección autorizadas
    del cliente OAuth en Google Cloud Console.

# El token generado queda en data/youtube_token.json.
# El contenedor lo lee desde /app/data/youtube_token.json (volumen montado).
"""

import json
import os
import sys
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

# Google devuelve scopes adicionales heredados de la configuración n8n
# (youtubepartner, youtube.force-ssl). Sin esto, google-auth-oauthlib
# lanza excepción por "scope mismatch" aunque la auth sea correcta.
os.environ["OAUTHLIB_RELAX_TOKEN_SCOPE"] = "1"

try:
    from google_auth_oauthlib.flow import Flow
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
except ImportError:
    print("❌ Faltan dependencias. Ejecuta en Windows:")
    print("   pip install google-api-python-client google-auth-oauthlib")
    sys.exit(1)

# ── Rutas ────────────────────────────────────────────────────────────────────
PROJECT_ROOT   = Path(os.getenv("PROJECT_ROOT", Path(__file__).parent.parent.parent.absolute()))
DATA_DIR       = Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data"))

CLIENT_SECRETS = PROJECT_ROOT / "config" / "youtube_client_secrets.json"
TOKEN_PATH     = DATA_DIR     / "youtube_token.json"
REDIRECT_URI   = "http://localhost:8080/"

SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]


class _CallbackHandler(BaseHTTPRequestHandler):
    auth_code = None
    error     = None

    def do_GET(self):
        params = parse_qs(urlparse(self.path).query)
        if "code" in params:
            _CallbackHandler.auth_code = params["code"][0]
            self._respond("✅ Autorización completada. Puedes cerrar esta pestaña.")
        elif "error" in params:
            _CallbackHandler.error = params["error"][0]
            self._respond(f"❌ Error: {params['error'][0]}")
        else:
            self._respond("⏳ Esperando autorización...")

    def _respond(self, message: str):
        body = f"<html><body><h2>{message}</h2></body></html>".encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


def refresh_if_valid() -> bool:
    if not TOKEN_PATH.exists():
        return False
    try:
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
        if creds.valid:
            print("✅ Token existente válido — no es necesario volver a autenticar.")
            return True
        if creds.expired and creds.refresh_token:
            print("🔄 Token expirado — renovando automáticamente...")
            creds.refresh(Request())
            TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")
            print("✅ Token renovado y guardado.")
            return True
    except Exception as e:
        print(f"⚠️  Token existente no reutilizable ({e}) — iniciando flujo completo.")
    return False


def main():
    print("=" * 55)
    print("🔐 AUTENTICADOR YOUTUBE OAUTH2 — canal-reli")
    print("=" * 55)

    if refresh_if_valid():
        return

    if not CLIENT_SECRETS.exists():
        print(f"\n❌ No se encuentra: {CLIENT_SECRETS}")
        print("   Coloca youtube_client_secrets.json en la carpeta config/")
        sys.exit(1)

    try:
        flow = Flow.from_client_secrets_file(
            str(CLIENT_SECRETS),
            scopes=SCOPES,
            redirect_uri=REDIRECT_URI
        )
    except Exception as e:
        print(f"\n❌ Error cargando credenciales: {e}")
        sys.exit(1)

    auth_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent"
    )

    print("\n📌 Se abrirá el navegador para que autorices el acceso.")
    print("   Si no se abre automáticamente, copia esta URL:")
    print(f"\n   {auth_url}\n")
    webbrowser.open(auth_url)

    print("⏳ Esperando callback en http://localhost:8080/ ...")
    server = HTTPServer(("localhost", 8080), _CallbackHandler)
    server.handle_request()

    if _CallbackHandler.error:
        print(f"\n❌ Error en la autorización: {_CallbackHandler.error}")
        sys.exit(1)

    if not _CallbackHandler.auth_code:
        print("\n❌ No se recibió código de autorización.")
        sys.exit(1)

    print("\n🔄 Intercambiando código por token de acceso...")
    try:
        flow.fetch_token(code=_CallbackHandler.auth_code)
    except Exception as e:
        print(f"❌ Error obteniendo token: {e}")
        print("\n💡 Causa más probable: http://localhost:8080/ no está en las")
        print("   URIs de redirección autorizadas. Añádela en Google Cloud Console")
        print("   → Credenciales → tu cliente OAuth → Editar → URIs de redirección.")
        sys.exit(1)

    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_PATH.write_text(flow.credentials.to_json(), encoding="utf-8")

    print(f"\n✅ Token guardado en: {TOKEN_PATH}")
    print("   El contenedor lo leerá desde /app/data/youtube_token.json")
    print("\n▶️  Siguiente paso:")
    print("   python app/pipeline/run_pipeline.py --fila 1")


if __name__ == "__main__":
    main()