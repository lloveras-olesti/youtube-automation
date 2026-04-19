#!/usr/bin/env python3
"""
Subir Portada a YouTube
========================
Asocia portada.jpg al vídeo recién subido usando thumbnails.set
de la YouTube Data API v3.

Requiere:
  - data/youtube_token.json con scope https://www.googleapis.com/auth/youtube
  - data/output/portadas/portada.jpg  (generado por generar_portada.py)
  - El video_id del vídeo ya subido (leído del output de subir_video.py)
  - Canal verificado por teléfono en youtube.com/verify

Uso:
    python app/utils/subir_portada.py --video-id ABC123xyz
    python app/utils/subir_portada.py --input data/output/metadata/upload_result_fila1.json
    python app/utils/subir_portada.py  # lee video_id del output más reciente

Ubicación: C:\\docker\\projects\\canal-reli\\app\\utils\\subir_portada.py
"""

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, "/app")

try:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    from googleapiclient.errors import HttpError
except ImportError:
    print("Faltan dependencias: pip install google-api-python-client google-auth-oauthlib")
    sys.exit(1)

try:
    from app.config import settings
    TOKEN_PATH    = settings.data_path / "youtube_token.json"
    PORTADA_PATH  = settings.output_path / "portadas" / "portada.jpg"
    METADATA_DIR  = settings.output_metadata_path
except Exception:
    # Fallback con rutas relativas al script (útil para pruebas locales)
    _ROOT        = Path(__file__).parent.parent.parent
    TOKEN_PATH   = _ROOT / "data" / "youtube_token.json"
    PORTADA_PATH = _ROOT / "data" / "output" / "portadas" / "portada.jpg"
    METADATA_DIR = _ROOT / "data" / "output" / "metadata"

SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


# =============================================================================
# AUTENTICACIÓN (igual que subir_video.py)
# =============================================================================

def get_youtube_client():
    """Carga el token OAuth y lo renueva si es necesario."""
    if not TOKEN_PATH.exists():
        logger.error(f"Token no encontrado: {TOKEN_PATH}")
        logger.error("Ejecuta en el host: python app/utils/auth_youtube.py")
        sys.exit(1)

    try:
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    except Exception as e:
        logger.error(f"Error leyendo token: {e}")
        sys.exit(1)

    if not creds.valid:
        if creds.expired and creds.refresh_token:
            logger.info("Renovando token OAuth...")
            creds.refresh(Request())
            TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")
            logger.info("Token renovado")
        else:
            logger.error("Token invalido y no renovable. Re-ejecuta auth_youtube.py en el host.")
            sys.exit(1)

    return build("youtube", "v3", credentials=creds)


# =============================================================================
# LOCALIZAR VIDEO ID
# =============================================================================

def obtener_video_id(video_id_arg: str | None, input_file: str | None) -> str:
    """
    Busca el video_id en este orden de prioridad:
    1. Argumento --video-id directo
    2. Archivo --input (JSON con campo video_id)
    3. Último upload_result_filaN.json en METADATA_DIR
    """
    if video_id_arg:
        return video_id_arg.strip()

    if input_file:
        ruta = Path(input_file)
        if not ruta.exists():
            logger.error(f"Archivo no encontrado: {ruta}")
            sys.exit(1)
        data = json.loads(ruta.read_text(encoding="utf-8"))
        vid  = data.get("video_id") or data.get("videoId") or data.get("id")
        if not vid:
            logger.error(f"No se encontró video_id en {ruta}. Campos disponibles: {list(data.keys())}")
            sys.exit(1)
        return vid.strip()

    # Buscar el más reciente en METADATA_DIR
    if METADATA_DIR.exists():
        resultados = sorted(METADATA_DIR.glob("upload_result_fila*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        for ruta in resultados:
            try:
                data = json.loads(ruta.read_text(encoding="utf-8"))
                vid  = data.get("video_id") or data.get("videoId") or data.get("id")
                if vid:
                    logger.info(f"video_id leido de: {ruta.name}")
                    return vid.strip()
            except Exception:
                continue

    logger.error(
        "No se pudo determinar el video_id.\n"
        "Usa --video-id ABC123 o --input ruta/upload_result.json"
    )
    sys.exit(1)


# =============================================================================
# SUBIDA DE THUMBNAIL
# =============================================================================

def subir_thumbnail(youtube, video_id: str, portada_path: Path) -> dict:
    """
    Llama a thumbnails.set con la imagen como upload multipart.

    Endpoint: POST https://www.googleapis.com/upload/youtube/v3/thumbnails/set
    Scope requerido: https://www.googleapis.com/auth/youtube
    Coste de quota: 50 unidades (igual que videos.insert)
    Tamaño máximo: 2 MB | Formatos: image/jpeg, image/png
    """
    if not portada_path.exists():
        logger.error(f"No se encuentra la portada: {portada_path}")
        logger.error("Ejecuta primero: python app/generators/generar_portada.py")
        sys.exit(1)

    tamano_mb = portada_path.stat().st_size / (1024 * 1024)
    if tamano_mb > 2.0:
        logger.error(f"La portada pesa {tamano_mb:.1f} MB. El limite de YouTube es 2 MB.")
        logger.error("Reduce la calidad del JPEG en generar_portada.py (quality=85)")
        sys.exit(1)

    logger.info(f"Portada: {portada_path.name} ({tamano_mb:.2f} MB)")
    logger.info(f"Video ID: {video_id}")

    media = MediaFileUpload(
        str(portada_path),
        mimetype="image/jpeg",
        resumable=False      # La imagen es pequeña, upload simple en una sola peticion
    )

    try:
        respuesta = youtube.thumbnails().set(
            videoId=video_id,
            media_body=media
        ).execute()

        url_thumbnail = (
            respuesta.get("items", [{}])[0]
            .get("maxres", respuesta.get("items", [{}])[0].get("high", {}))
            .get("url", "(URL no disponible)")
        )

        logger.info("Thumbnail subido correctamente")
        logger.info(f"  URL: {url_thumbnail}")

        return {
            "video_id":  video_id,
            "thumbnail": url_thumbnail,
            "portada":   str(portada_path),
        }

    except HttpError as e:
        cuerpo = e.content.decode("utf-8") if e.content else str(e)

        # Errores conocidos con mensajes claros
        if e.resp.status == 403:
            if "forbidden" in cuerpo.lower() or "thumbnailsNotAllowed" in cuerpo.lower():
                logger.error("ERROR 403: El canal no tiene habilitados los thumbnails personalizados.")
                logger.error("  Solución: Verifica el canal en https://www.youtube.com/verify")
            else:
                logger.error(f"ERROR 403 (sin permisos): {cuerpo}")
                logger.error("  Puede que el token no tenga el scope 'youtube'. Re-ejecuta auth_youtube.py")
        elif e.resp.status == 404:
            logger.error(f"ERROR 404: Video '{video_id}' no encontrado o no pertenece a este canal.")
        else:
            logger.error(f"Error HTTP {e.resp.status}: {cuerpo}")

        sys.exit(1)


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Sube portada a YouTube")
    parser.add_argument("--video-id", default=None,
                        help="ID del video de YouTube (ej. dQw4w9WgXcQ)")
    parser.add_argument("--input",    default=None,
                        help="JSON con campo video_id (output de subir_video.py)")
    parser.add_argument("--portada",  default=None,
                        help="Ruta alternativa a la imagen de portada")
    args = parser.parse_args()

    logger.info("=" * 50)
    logger.info("SUBIR PORTADA A YOUTUBE — canal-reli")
    logger.info("=" * 50)

    # Portada
    portada = Path(args.portada) if args.portada else PORTADA_PATH

    # Video ID
    video_id = obtener_video_id(args.video_id, args.input)

    # Cliente YouTube
    logger.info("Cargando credenciales OAuth...")
    youtube = get_youtube_client()

    # Subir
    logger.info("Subiendo thumbnail...")
    resultado = subir_thumbnail(youtube, video_id, portada)

    # Guardar resultado (para trazabilidad)
    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    out = METADATA_DIR / "thumbnail_result.json"
    out.write_text(json.dumps(resultado, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"Resultado guardado en: {out.name}")

    # Salida estándar para que run_pipeline.py pueda capturarla si es necesario
    print(json.dumps(resultado))


if __name__ == "__main__":
    main()
