#!/usr/bin/env python3
"""
Subir Vídeo a YouTube
======================
Sube el vídeo generado a YouTube usando el token OAuth almacenado
en data/youtube_token.json (generado por scripts/auth_youtube.py en el host).

El vídeo se sube como PRIVADO y se programa para publicarse a las 12:00 hora
española en la fecha indicada en el campo 'fecha' de los metadatos.

Uso (desde el runner, dentro del contenedor):
  python app/utils/subir_video.py --fila 1
  python app/utils/subir_video.py --fila 1 --input /app/data/temp/pipeline/procesar-metadata_output.json
"""

import argparse
import json
import logging
import sys
from datetime import datetime, time as dtime
from pathlib import Path

sys.path.insert(0, "/app")
from app.config import settings

try:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    from googleapiclient.errors import HttpError
except ImportError:
    print("Faltan dependencias: google-api-python-client google-auth-oauthlib")
    sys.exit(1)

# Zona horaria España: zoneinfo (Python 3.9+) con fallback a pytz
try:
    from zoneinfo import ZoneInfo
    MADRID_TZ = ZoneInfo("Europe/Madrid")
except ImportError:
    try:
        import pytz
        MADRID_TZ = pytz.timezone("Europe/Madrid")
    except ImportError:
        MADRID_TZ = None

TOKEN_PATH   = settings.data_path / "youtube_token.json"
METADATA_DIR = settings.output_metadata_path
VIDEOS_DIR   = settings.output_videos_path
LOGS_DIR     = settings.logs_path / "subida"

SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]

LOGS_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def calcular_publish_at(fecha_str: str):
    """
    Calcula el datetime de publicacion a las 12:00 hora espanola.

    Args:
        fecha_str: Fecha en formato YYYY-MM-DD (ej. "2026-03-06")

    Returns:
        String ISO 8601 con timezone para la API de YouTube.
        None si la fecha no es valida o ya ha pasado.

    Notas:
        - Espana usa CET (UTC+1) en invierno y CEST (UTC+2) en verano.
        - zoneinfo gestiona el cambio de hora automaticamente.
        - La API de YouTube requiere publishAt FUTURO.
        - El video debe subirse con privacyStatus="private" para programarse.
    """
    if not fecha_str:
        return None

    try:
        fecha = datetime.strptime(fecha_str.strip(), "%Y-%m-%d").date()
    except ValueError:
        logger.warning(f"Formato de fecha no reconocido: '{fecha_str}'. Se esperaba YYYY-MM-DD.")
        return None

    if MADRID_TZ is None:
        # Fallback sin timezone: 12:00 UTC
        dt_naive = datetime.combine(fecha, dtime(12, 0, 0))
        logger.warning("Sin timezone configurada, usando 12:00 UTC")
        return dt_naive.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    dt_madrid = datetime.combine(fecha, dtime(12, 0, 0)).replace(tzinfo=MADRID_TZ)
    now_madrid = datetime.now(tz=MADRID_TZ)

    if dt_madrid <= now_madrid:
        logger.warning(
            f"La fecha {fecha_str} 12:00 ya ha pasado. "
            f"El video se subira como privado sin programacion automatica."
        )
        return None

    return dt_madrid.isoformat()


def get_youtube_client():
    if not TOKEN_PATH.exists():
        logger.error(f"Token no encontrado: {TOKEN_PATH}")
        logger.error("Ejecuta en el host: python scripts/auth_youtube.py")
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


def load_metadata(fila: int, input_file):
    """
    Carga los metadatos del video.
    Prioridad:
      1. --input (output de procesar-metadata del pipeline)
      2. metadata_{fila}.json en data/output/metadata/
    """
    if input_file:
        path = Path(input_file)
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if data:  # no aceptar {} vacio
                    logger.info(f"Metadatos cargados desde pipeline: {path.name}")
                    return data
                else:
                    logger.warning(f"El archivo de input esta vacio: {path.name}")
            except json.JSONDecodeError as e:
                logger.warning(f"Error parseando input del pipeline: {e}")

    meta_path = METADATA_DIR / f"metadata_{fila}.json"
    if meta_path.exists():
        data = json.loads(meta_path.read_text(encoding="utf-8"))
        logger.info(f"Metadatos cargados desde: {meta_path.name}")
        return data

    logger.error(f"No se encontraron metadatos para la fila {fila}")
    logger.error(f"Buscado en: {meta_path}")
    sys.exit(1)


def find_video_file(fila: int, metadata: dict) -> Path:
    posible_nombre = metadata.get("video_filename") or metadata.get("filename")
    if not posible_nombre and "_raw" in metadata:
        posible_nombre = (
            metadata["_raw"].get("video_filename")
            or metadata["_raw"].get("filename")
        )

    if posible_nombre:
        p = VIDEOS_DIR / posible_nombre
        if p.exists():
            return p

    for patron in [f"video_{fila}.mp4", f"output_{fila}.mp4", f"video_fila{fila}.mp4"]:
        p = VIDEOS_DIR / patron
        if p.exists():
            return p

    videos = list(VIDEOS_DIR.glob("*.mp4"))
    if len(videos) == 1:
        logger.info(f"Usando unico video encontrado: {videos[0].name}")
        return videos[0]

    if len(videos) > 1:
        logger.error(f"Multiples videos en {VIDEOS_DIR}. No se cual subir.")
        logger.error(f"Videos encontrados: {[v.name for v in videos]}")
    else:
        logger.error(f"No se encontro ningun archivo .mp4 en {VIDEOS_DIR}")

    sys.exit(1)


def upload_video(youtube, video_path: Path, metadata: dict) -> dict:
    """
    Sube el video a YouTube.

    Siempre sube como PRIVADO. Si hay fecha valida y futura, anade publishAt
    para que YouTube lo publique automaticamente a las 12:00 hora espanola.

    IMPORTANTE: La API de YouTube exige privacyStatus="private" cuando se usa
    publishAt. Un video publico con publishAt es rechazado por la API.

    La subida usa upload resumable con reintentos automaticos ante cualquier
    error de red (TimeoutError, ConnectionReset, etc.) o error HTTP transitorio
    (5xx). Backoff exponencial: 2s, 4s, 8s... hasta MAX_RETRIES intentos.
    """
    import time
    import socket

    MAX_RETRIES = 10
    HTTP_RETRY_CODES = {500, 502, 503, 504}
    NETWORK_ERRORS = (
        TimeoutError,
        ConnectionResetError,
        ConnectionAbortedError,
        BrokenPipeError,
        OSError,
        socket.timeout,
    )

    title       = metadata.get("titulo") or metadata.get("title", "Video")
    description = metadata.get("descripcion") or metadata.get("description", "")
    
    raw_tags = metadata.get("tags") or metadata.get("etiquetas") or []
    if isinstance(raw_tags, str):
        raw_tags = [t.strip() for t in raw_tags.split(",") if t.strip()]

    tags = []
    current_length = 0
    for t in raw_tags:
        t = t.replace("<", "").replace(">", "").replace('"', '').strip()
        if not t:
            continue
        if len(t) > 48:
            t = t[:48]
            
        costo_tag = len(t) + 4
        if current_length + costo_tag < 450:
            tags.append(t)
            current_length += costo_tag
        else:
            logger.warning(f"  Omitiendo tag: '{t}' (límite superado)")

    category_id = str(metadata.get("category_id", "22"))

    # Fecha: puede venir como campo directo o dentro de _raw
    fecha_str = (
        metadata.get("fecha")
        or metadata.get("_raw", {}).get("fecha", "")
        or ""
    )
    publish_at = calcular_publish_at(fecha_str) if fecha_str else None

    if publish_at:
        logger.info(f"Programado para publicarse: {publish_at} (12:00 hora espanola)")
    else:
        logger.warning("Sin fecha de programacion valida. El video quedara como privado.")

    body = {
        "snippet": {
            "title":           title,
            "description":     description,
            "tags":            tags,
            "categoryId":      category_id,
            "defaultLanguage": "es",
        },
        "status": {
            # Siempre private: obligatorio para publishAt y mas seguro en general
            "privacyStatus":           "private",
            "selfDeclaredMadeForKids": False,
        }
    }

    if publish_at:
        body["status"]["publishAt"] = publish_at

    logger.info(f"Subiendo: {video_path.name}")
    logger.info(f"  Titulo:    {title}")
    logger.info(f"  Tags:      {len(tags)} etiquetas")
    logger.info(f"  Estado:    private" + (f" | se publica {fecha_str} 12:00 ES" if publish_at else ""))

    media = MediaFileUpload(
        str(video_path),
        mimetype="video/mp4",
        resumable=True,
        chunksize=20 * 1024 * 1024  # 20 MB por chunk (antes 10 MB)
    )

    logger.info(f"  Tags completos: {tags}")

    request = youtube.videos().insert(
        part="snippet,status",
        body=body,
        media_body=media
    )

    response = None
    last_progress = -1
    retry = 0

    while response is None:
        try:
            status, response = request.next_chunk()
            if status:
                progress = int(status.progress() * 100)
                if progress != last_progress and progress % 10 == 0:
                    logger.info(f"  Progreso: {progress}%")
                    last_progress = progress
            retry = 0  # reset contador en cada chunk exitoso

        except HttpError as e:
            if e.resp.status in HTTP_RETRY_CODES:
                retry += 1
                if retry > MAX_RETRIES:
                    raise Exception(f"Maximo de reintentos alcanzado tras error HTTP {e.resp.status}")
                wait = min(2 ** retry, 120)
                logger.warning(f"  Error HTTP {e.resp.status} — reintento {retry}/{MAX_RETRIES} en {wait}s...")
                time.sleep(wait)
            else:
                raise

        except NETWORK_ERRORS as e:
            retry += 1
            if retry > MAX_RETRIES:
                raise Exception(f"Maximo de reintentos alcanzado tras error de red: {e}")
            wait = min(2 ** retry, 120)
            logger.warning(
                f"  Error de red ({type(e).__name__}: {e}) — "
                f"reintento {retry}/{MAX_RETRIES} en {wait}s..."
            )
            time.sleep(wait)

    video_id  = response["id"]
    video_url = f"https://www.youtube.com/watch?v={video_id}"

    logger.info(f"Video subido correctamente")
    logger.info(f"  ID:  {video_id}")
    logger.info(f"  URL: {video_url}")
    if publish_at:
        logger.info(f"  Se publicara automaticamente el {fecha_str} a las 12:00 hora espanola")

    return {
        "video_id":   video_id,
        "url":        video_url,
        "title":      title,
        "publish_at": publish_at,
        "fecha":      fecha_str,
    }


def main():
    parser = argparse.ArgumentParser(description="Sube video a YouTube")
    parser.add_argument("--fila",  type=int, default=1)
    parser.add_argument("--input", type=str, default=None)
    args = parser.parse_args()

    logger.info("=" * 50)
    logger.info("SUBIDA A YOUTUBE - canal-reli")
    logger.info("=" * 50)

    logger.info("\nCargando credenciales OAuth...")
    youtube = get_youtube_client()

    logger.info("\nCargando metadatos...")
    metadata = load_metadata(args.fila, args.input)

    logger.info("\nLocalizando archivo de video...")
    video_path = find_video_file(args.fila, metadata)
    size_mb = video_path.stat().st_size / (1024 * 1024)
    logger.info(f"  Archivo: {video_path.name} ({size_mb:.1f} MB)")

    logger.info("\nIniciando subida...")
    result = upload_video(youtube, video_path, metadata)

    output_path = METADATA_DIR / f"upload_result_fila{args.fila}.json"
    output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"\nResultado guardado en: {output_path.name}")

    print(json.dumps(result))


if __name__ == "__main__":
    main()