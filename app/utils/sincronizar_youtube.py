#!/usr/bin/env python3
"""
Sincronizador YouTube Analytics → historico_videos.csv
=======================================================
Rellena CTR, IMPRESIONES y VISUALIZACIONES en historico_videos.csv
para las filas que aún no tienen datos de analytics.

Lógica de funcionamiento:
  1. Lee historico_videos.csv
  2. Detecta la primera fila sin analytics (CTR vacío)
  3. Determina el rango de fechas: desde esa fila hasta la última del CSV
  4. Consulta YouTube Analytics API para ese rango
  5. Cruza por título y rellena las columnas de analytics
  6. Guarda el CSV actualizado (o solo imprime si --dry-run)

IMPORTANTE — Autenticación:
  - CTR, impresiones y visualizaciones son datos PRIVADOS del canal.
  - Requieren OAuth 2.0, NO basta con YOUTUBE_API_KEY.
  - El token se guarda en config/youtube_token.json y se renueva automáticamente.

APIs necesarias (habilitar en Google Cloud Console):
  - YouTube Data API v3         → metadatos de videos (títulos, fechas)
  - YouTube Analytics API       → CTR, impresiones, visualizaciones

Uso:
  python sincronizar_youtube.py              # rellena filas sin analytics
  python sincronizar_youtube.py --dry-run    # solo muestra qué haría, sin modificar nada
  python sincronizar_youtube.py --mes 2026-02  # fuerza un rango de mes concreto
  python sincronizar_youtube.py --force      # reescribe aunque ya haya datos

Ubicación: C:\\docker\\projects\\canal-reli\\app\\utils\\sincronizar_youtube.py
"""

import os
import sys
import json
import logging
import argparse
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

# Google API
try:
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
except ImportError:
    print("❌ Faltan dependencias de Google API.")
    print("   Ejecuta: pip install google-api-python-client google-auth-oauthlib python-dateutil")
    sys.exit(1)

# Rutas del proyecto
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from app.config import settings

# ============================================
# CONSTANTES
# ============================================

SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly"
]

TOKEN_FILE          = settings.token_path
CLIENT_SECRETS_FILE = settings.config_path / "youtube_client_secrets.json"
HISTORICO_CSV       = settings.historico_path

# Columnas de analytics que este script rellena
COLS_ANALYTICS = ["CTR", "IMPRESIONES", "VISUALIZACIONES"]

# ============================================
# LOGGING
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


# ============================================
# AUTENTICACIÓN OAUTH
# ============================================

def get_credentials() -> Credentials:
    """
    Obtiene/renueva credenciales OAuth2.
    Primera ejecución: abre navegador para autorización.
    Ejecuciones posteriores: carga token guardado y lo renueva si es necesario.

    Para generar youtube_client_secrets.json:
    1. Ve a Google Cloud Console → tu proyecto → APIs y servicios → Credenciales
    2. Crear credenciales → ID de cliente OAuth 2.0 → Aplicación de escritorio
    3. Descarga el JSON y renómbralo a youtube_client_secrets.json
    4. Colócalo en config/
    """
    creds = None

    if TOKEN_FILE.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
        except Exception as e:
            logger.warning(f"⚠️  Token existente no válido: {e}")
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("🔄 Renovando token OAuth...")
            try:
                creds.refresh(Request())
                logger.info("✅ Token renovado correctamente")
            except Exception as e:
                logger.warning(f"⚠️  No se pudo renovar: {e} — iniciando autenticación completa")
                creds = None

        if not creds:
            if not CLIENT_SECRETS_FILE.exists():
                logger.error(f"❌ No se encuentra: {CLIENT_SECRETS_FILE}")
                logger.error("   Descarga tus credenciales OAuth desde Google Cloud Console")
                sys.exit(1)

            logger.info("🌐 Abriendo navegador para autorización OAuth...")
            flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRETS_FILE), SCOPES)
            creds = flow.run_local_server(port=0)
            logger.info("✅ Autorización completada")

        TOKEN_FILE.write_text(creds.to_json(), encoding="utf-8")
        logger.info(f"💾 Token guardado en: {TOKEN_FILE.name}")

    return creds


# ============================================
# DETECTAR RANGO A SINCRONIZAR
# ============================================

def detectar_rango(df: pd.DataFrame, mes_forzado: str | None, force: bool) -> tuple[str, str] | None:
    """
    Determina el rango de fechas a sincronizar.

    Modos:
    - --mes YYYY-MM: usa ese mes exacto, independientemente del estado del CSV.
    - normal:        busca la primera fila sin CTR y va hasta la última fila del CSV.
    - --force:       trata todas las filas como sin datos (rango completo del CSV).

    Retorna (fecha_inicio, fecha_fin) en formato YYYY-MM-DD, o None si no hay nada que hacer.
    """
    if mes_forzado:
        mes_dt = datetime.strptime(mes_forzado, "%Y-%m")
        fecha_inicio = mes_dt.strftime("%Y-%m-01")
        ultimo_dia = (mes_dt + relativedelta(months=1) - relativedelta(days=1)).day
        fecha_fin = mes_dt.strftime(f"%Y-%m-{ultimo_dia:02d}")
        logger.info(f"📅 Rango forzado por --mes: {fecha_inicio} → {fecha_fin}")
        return fecha_inicio, fecha_fin

    if force:
        fecha_inicio = df["FECHA"].min()
        fecha_fin = df["FECHA"].max()
        logger.info(f"📅 Rango completo (--force): {fecha_inicio} → {fecha_fin}")
        return fecha_inicio, fecha_fin

    # Detectar primera fila sin analytics
    sin_datos = df[df["CTR"].isna() | (df["CTR"] == "")]
    if sin_datos.empty:
        logger.info("✅ Todas las filas ya tienen analytics — nada que sincronizar")
        logger.info("   Usa --force para reescribir igualmente, o --mes YYYY-MM para un mes concreto")
        return None

    fecha_inicio = sin_datos["FECHA"].min()
    fecha_fin = df["FECHA"].max()
    logger.info(f"📅 Rango detectado automáticamente: {fecha_inicio} → {fecha_fin}")
    logger.info(f"   ({len(sin_datos)} filas sin analytics en ese período)")
    return fecha_inicio, fecha_fin


# ============================================
# OBTENER ID DEL CANAL
# ============================================

def get_channel_id(youtube) -> str:
    response = youtube.channels().list(part="id,snippet", mine=True).execute()
    if not response.get("items"):
        logger.error("❌ No se encontró el canal")
        sys.exit(1)
    channel = response["items"][0]
    channel_id = channel["id"]
    logger.info(f"📺 Canal: {channel['snippet']['title']} ({channel_id})")
    return channel_id


# ============================================
# OBTENER VIDEOS DEL RANGO
# ============================================

def get_videos_in_range(youtube, channel_id: str, fecha_inicio: str, fecha_fin: str) -> list[dict]:
    """
    Obtiene videos publicados en el rango dado.
    Retorna lista de dicts con video_id, titulo, fecha_publicacion.
    """
    videos = []
    page_token = None

    logger.info(f"🔍 Buscando videos entre {fecha_inicio} y {fecha_fin}...")

    while True:
        params = {
            "part": "id,snippet",
            "channelId": channel_id,
            "publishedAfter": f"{fecha_inicio}T00:00:00Z",
            "publishedBefore": f"{fecha_fin}T23:59:59Z",
            "maxResults": 50,
            "type": "video",
            "order": "date"
        }
        if page_token:
            params["pageToken"] = page_token

        response = youtube.search().list(**params).execute()

        for item in response.get("items", []):
            videos.append({
                "video_id": item["id"]["videoId"],
                "titulo": item["snippet"]["title"],
                "fecha_publicacion": item["snippet"]["publishedAt"][:10]
            })

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    logger.info(f"📹 {len(videos)} videos encontrados en YouTube para ese rango")
    return videos


# ============================================
# OBTENER ANALYTICS POR VIDEO
# ============================================

def get_analytics_for_videos(
    yt_analytics,
    channel_id: str,
    video_ids: list[str],
    fecha_inicio: str,
    fecha_fin: str
) -> dict:
    """
    Consulta YouTube Analytics API para CTR, impresiones y visualizaciones.
    Procesa en lotes de 10 (límite de la API para filtros por video).

    Retorna: {video_id: {ctr, impresiones, visualizaciones}}
    """
    metrics = "impressions,views,clickThroughRate"
    resultados = {}
    batch_size = 10
    batches = [video_ids[i:i + batch_size] for i in range(0, len(video_ids), batch_size)]

    for i, batch in enumerate(batches):
        filter_str = "video==" + ",".join(batch)
        try:
            response = yt_analytics.reports().query(
                ids=f"channel=={channel_id}",
                startDate=fecha_inicio,
                endDate=fecha_fin,
                metrics=metrics,
                dimensions="video",
                filters=filter_str
            ).execute()

            headers = [h["name"] for h in response.get("columnHeaders", [])]
            for row in response.get("rows", []):
                row_dict = dict(zip(headers, row))
                vid = row_dict.get("video", "")
                resultados[vid] = {
                    "impresiones": int(row_dict.get("impressions", 0)),
                    "visualizaciones": int(row_dict.get("views", 0)),
                    # La API devuelve CTR como fracción (0.05 = 5%) → convertimos a %
                    "ctr": round(float(row_dict.get("clickThroughRate", 0)) * 100, 2)
                }
        except Exception as e:
            logger.warning(f"⚠️  Error en lote {i+1}: {e}")

    logger.info(f"📊 Analytics obtenidos para {len(resultados)} videos")
    return resultados


# ============================================
# ACTUALIZAR HISTÓRICO CSV
# ============================================

def update_historico(
    df: pd.DataFrame,
    videos: list[dict],
    analytics: dict,
    force: bool,
    dry_run: bool
) -> pd.DataFrame:
    """
    Rellena CTR, IMPRESIONES y VISUALIZACIONES en el DataFrame.

    Estrategia de cruce:
    - Primero intenta cruzar por título exacto (más fiable).
    - Si no encuentra, registra una advertencia.
    
    En dry-run: imprime los cambios previstos pero NO modifica el DataFrame.
    """
    actualizados = 0
    no_encontrados = 0

    # Construir mapa título → analytics desde YouTube
    titulo_a_stats: dict[str, dict] = {}
    for video in videos:
        vid_id = video["video_id"]
        if vid_id in analytics:
            titulo_a_stats[video["titulo"]] = analytics[vid_id]

    if dry_run:
        logger.info("\n🔍 [DRY RUN] — simulando cambios sin modificar el CSV:")

    for idx, row in df.iterrows():
        tiene_datos = pd.notna(row.get("CTR")) and str(row.get("CTR", "")).strip() != ""
        if tiene_datos and not force:
            continue

        titulo = row["TÍTULO"]
        stats = titulo_a_stats.get(titulo)

        if not stats:
            # Título no encontrado en YouTube — puede ser video futuro o título modificado
            no_encontrados += 1
            continue

        if dry_run:
            logger.info(
                f"  ✏️  [{row['FECHA']}] {titulo[:60]}\n"
                f"       CTR: {stats['ctr']}% | Imp: {stats['impresiones']} | Vis: {stats['visualizaciones']}"
            )
        else:
            df.at[idx, "CTR"]           = stats["ctr"]
            df.at[idx, "IMPRESIONES"]   = stats["impresiones"]
            df.at[idx, "VISUALIZACIONES"] = stats["visualizaciones"]

        actualizados += 1

    if dry_run:
        logger.info(f"\n📊 [DRY RUN] Se actualizarían {actualizados} filas")
        if no_encontrados:
            logger.info(f"   ⚠️  {no_encontrados} filas no encontradas en YouTube (pueden ser vídeos futuros)")
    else:
        logger.info(f"✅ Filas actualizadas: {actualizados}")
        if no_encontrados:
            logger.warning(f"⚠️  {no_encontrados} filas no encontradas en YouTube por título")
            logger.warning("   Posibles causas: vídeos futuros, títulos modificados en YouTube Studio")

    return df


# ============================================
# FUNCIÓN PRINCIPAL
# ============================================

def main():
    parser = argparse.ArgumentParser(description="Sincroniza YouTube Analytics → historico_videos.csv")
    parser.add_argument("--mes", type=str, default=None,
                        help="Fuerza un mes concreto YYYY-MM en lugar de detectar automáticamente")
    parser.add_argument("--force", action="store_true",
                        help="Reescribe analytics aunque la fila ya tenga datos")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo muestra qué cambiaría — NO modifica historico_videos.csv")
    args = parser.parse_args()

    env_path = CONFIG_DIR.parent / ".env"
    load_dotenv(env_path)

    logger.info("=" * 60)
    logger.info("📡 SINCRONIZADOR YOUTUBE ANALYTICS")
    if args.dry_run:
        logger.info("   ⚠️  MODO DRY-RUN — no se modificará ningún archivo")
    logger.info("=" * 60)

    # ── Cargar histórico ──────────────────────────────────────────
    if not HISTORICO_CSV.exists():
        logger.error(f"❌ No se encuentra historico_videos.csv en: {HISTORICO_CSV}")
        sys.exit(1)

    df = pd.read_csv(HISTORICO_CSV, encoding="utf-8", dtype=str)
    logger.info(f"📂 Histórico cargado: {len(df)} filas")

    # ── Detectar rango a sincronizar ──────────────────────────────
    rango = detectar_rango(df, args.mes, args.force)
    if rango is None:
        # Nada que hacer — salida limpia
        return
    fecha_inicio, fecha_fin = rango

    # ── Autenticar OAuth ──────────────────────────────────────────
    logger.info("\n🔐 Verificando autenticación OAuth...")
    creds = get_credentials()
    logger.info("✅ Autenticación correcta")

    if args.dry_run:
        logger.info("\n✅ [DRY RUN] OAuth verificado — el script se detiene aquí sin consultar la API")
        logger.info("   Elimina --dry-run para ejecutar la sincronización completa")
        return

    # ── Construir clientes API ────────────────────────────────────
    youtube     = build("youtube", "v3", credentials=creds)
    yt_analytics = build("youtubeAnalytics", "v2", credentials=creds)

    channel_id = get_channel_id(youtube)

    # ── Obtener videos del rango ──────────────────────────────────
    videos = get_videos_in_range(youtube, channel_id, fecha_inicio, fecha_fin)
    if not videos:
        logger.warning("⚠️  No se encontraron videos en YouTube para ese rango")
        return

    # ── Obtener analytics ─────────────────────────────────────────
    logger.info("\n📊 Consultando YouTube Analytics API...")
    video_ids = [v["video_id"] for v in videos]
    analytics = get_analytics_for_videos(yt_analytics, channel_id, video_ids, fecha_inicio, fecha_fin)

    # ── Actualizar DataFrame ──────────────────────────────────────
    logger.info("\n💾 Aplicando cambios al histórico...")
    df = update_historico(df, videos, analytics, force=args.force, dry_run=False)

    # ── Guardar CSV ───────────────────────────────────────────────
    df.to_csv(HISTORICO_CSV, index=False, encoding="utf-8")
    logger.info(f"💾 historico_videos.csv guardado ({len(df)} filas totales)")
    logger.info("\n✅ Sincronización completada")
    logger.info("   Siguiente paso: aprendizaje_mensual.py")


if __name__ == "__main__":
    main()