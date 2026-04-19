#!/usr/bin/env python3
"""
Obtener Referencias de Canales YouTube
=======================================
Extrae títulos + visualizaciones recientes de los canales de referencia
configurados en config/config_calendario.yaml y los guarda en
config/referencias_canales.json para uso automático en generar_calendario.py.

Fuentes de datos:
  - YouTube Data API v3 (pública, solo API key) → títulos + visualizaciones exactas
  - Claude web_search (fallback) → si falla la API o no hay API key

Uso:
  python obtener_referencias.py                  # configuración por defecto (10 videos/canal)
  python obtener_referencias.py --max-videos 20  # personaliza nº de videos por canal
  python obtener_referencias.py --solo-claude    # fuerza uso de Claude web_search (sin API)

Cuándo ejecutarlo:
  - Una vez al mes antes de generar el calendario.
  - generar_calendario.py carga el JSON automáticamente si existe.

Ubicación: C:\\docker\\projects\\canal-reli\\app\\generators\\obtener_referencias.py
"""

import os
import sys
import json
import yaml
import logging
import argparse
import time
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# ============================================
# RUTAS — calculadas desde la ubicación del script
# ============================================
PROJECT_ROOT     = Path(__file__).parent.parent.parent
CONFIG_DIR       = PROJECT_ROOT / "config"
RUNTIME_DIR      = Path(os.environ.get("RUNTIME_PATH", str(PROJECT_ROOT / "runtime")))
CONFIG_YAML      = CONFIG_DIR / "config_calendario.yaml"
REFERENCIAS_JSON = RUNTIME_DIR / "referencias_canales.json"
LOGS_DIR         = PROJECT_ROOT / "data" / "logs" / "referencias"

# Cargar .env ANTES de cualquier otro import que use variables de entorno
load_dotenv(PROJECT_ROOT / ".env")

import anthropic

# ============================================
# LOGGING
# ============================================
LOGS_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            LOGS_DIR / f"referencias_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.log",
            encoding="utf-8"
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================
# CARGA DE CONFIG
# ============================================

def load_config() -> dict:
    if not CONFIG_YAML.exists():
        logger.error(f"❌ No se encuentra config: {CONFIG_YAML}")
        sys.exit(1)
    with open(CONFIG_YAML, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ============================================
# YOUTUBE DATA API V3 — extracción directa
# ============================================

def get_channel_id_from_handle(handle: str, api_key: str) -> str | None:
    """
    Obtiene el channelId a partir del handle (@nombre).
    Usa el endpoint channels.list con forHandle.
    """
    import urllib.request
    import urllib.parse

    handle_clean = handle.lstrip("@")
    url = (
        "https://www.googleapis.com/youtube/v3/channels"
        f"?part=id,snippet&forHandle={urllib.parse.quote(handle_clean)}&key={api_key}"
    )
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
        items = data.get("items", [])
        if items:
            channel_id = items[0]["id"]
            logger.info(f"    📍 channelId: {channel_id}")
            return channel_id
        logger.warning(f"    ⚠️  No se encontró channelId para handle: {handle}")
        return None
    except Exception as e:
        logger.warning(f"    ⚠️  Error obteniendo channelId: {e}")
        return None


def get_latest_video_ids(channel_id: str, api_key: str, max_videos: int) -> list[str]:
    """
    Obtiene los IDs de los últimos N vídeos del canal usando search.list.
    """
    import urllib.request
    import urllib.parse

    url = (
        "https://www.googleapis.com/youtube/v3/search"
        f"?part=id&channelId={channel_id}&maxResults={max_videos}"
        f"&order=date&type=video&key={api_key}"
    )
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
        ids = [item["id"]["videoId"] for item in data.get("items", [])]
        logger.info(f"    🎬 {len(ids)} vídeos encontrados")
        return ids
    except Exception as e:
        logger.warning(f"    ⚠️  Error obteniendo vídeos: {e}")
        return []


def get_video_details(video_ids: list[str], api_key: str) -> list[dict]:
    """
    Obtiene título y viewCount para una lista de video IDs.
    Usa videos.list con parts snippet + statistics.
    Procesa en lotes de 50 (límite de la API).
    """
    import urllib.request

    resultados = []
    batch_size = 50

    for i in range(0, len(video_ids), batch_size):
        batch = video_ids[i:i + batch_size]
        ids_str = ",".join(batch)
        url = (
            "https://www.googleapis.com/youtube/v3/videos"
            f"?part=snippet,statistics&id={ids_str}&key={api_key}"
        )
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())
            for item in data.get("items", []):
                titulo = item["snippet"]["title"]
                views = int(item["statistics"].get("viewCount", 0))
                resultados.append({
                    "titulo": titulo,
                    "visualizaciones": views
                })
        except Exception as e:
            logger.warning(f"    ⚠️  Error obteniendo detalles de vídeos: {e}")

    return resultados


def fetch_channel_data_via_api(canal: dict, max_videos: int, api_key: str) -> dict:
    """
    Extrae títulos + visualizaciones usando YouTube Data API v3 (sin OAuth).
    Flujo: handle → channelId → video IDs → títulos + views
    """
    nombre = canal["nombre"]
    handle = canal["handle"]
    logger.info(f"  📡 [API] {nombre} ({handle})")

    channel_id = get_channel_id_from_handle(handle, api_key)
    if not channel_id:
        return {
            "canal": nombre, "handle": handle, "url": canal.get("url", ""),
            "videos": [], "total_encontrados": 0,
            "fuente": "api_error",
            "fecha_extraccion": datetime.now().strftime("%Y-%m-%d"),
            "error": "No se pudo obtener channelId"
        }

    video_ids = get_latest_video_ids(channel_id, api_key, max_videos)
    if not video_ids:
        return {
            "canal": nombre, "handle": handle, "url": canal.get("url", ""),
            "videos": [], "total_encontrados": 0,
            "fuente": "api_error",
            "fecha_extraccion": datetime.now().strftime("%Y-%m-%d"),
            "error": "No se obtuvieron video IDs"
        }

    videos = get_video_details(video_ids, api_key)
    logger.info(f"    ✅ {len(videos)} vídeos con datos completos")

    return {
        "canal": nombre,
        "handle": handle,
        "url": canal.get("url", ""),
        "videos": videos,                          # lista de {titulo, visualizaciones}
        "titulos": [v["titulo"] for v in videos],  # campo legacy para compatibilidad
        "total_encontrados": len(videos),
        "fuente": "youtube_data_api_v3",
        "fecha_extraccion": datetime.now().strftime("%Y-%m-%d")
    }


# ============================================
# FALLBACK — Claude web_search (sin API key)
# ============================================

def fetch_channel_data_via_claude(canal: dict, max_videos: int, client: anthropic.Anthropic) -> dict:
    """
    Fallback: usa Claude con web_search cuando no hay YOUTUBE_API_KEY.
    Obtiene títulos pero NO visualizaciones exactas (limitación del fallback).
    """
    nombre = canal["nombre"]
    handle = canal["handle"]
    logger.info(f"  🤖 [Claude] {nombre} ({handle})")

    prompt = f"""Busca en YouTube los videos más recientes del canal cristiano "{nombre}" (handle: {handle}).
Necesito los últimos {max_videos} títulos de video publicados en ese canal.

1. Busca "{handle} youtube" o "{nombre} oraciones youtube"
2. Extrae los títulos exactos incluyendo emojis
3. Si encuentras el número de visualizaciones, inclúyelo. Si no, pon 0.
4. No inventes títulos — solo los que encuentres realmente

Responde ÚNICAMENTE con JSON válido sin markdown:
{{
  "canal": "{nombre}",
  "handle": "{handle}",
  "videos": [
    {{"titulo": "Título exacto 1", "visualizaciones": 15000}},
    {{"titulo": "Título exacto 2", "visualizaciones": 0}}
  ],
  "fuente": "descripción de dónde obtuviste los datos"
}}"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2000,
            tools=[{"type": "web_search_20250305", "name": "web_search"}],
            messages=[{"role": "user", "content": prompt}]
        )
        text = "".join(b.text for b in message.content if b.type == "text").strip()
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()

        data = json.loads(text)
        videos = data.get("videos", [])
        logger.info(f"    ✅ {len(videos)} vídeos obtenidos (sin visualizaciones exactas)")
        return {
            "canal": nombre,
            "handle": handle,
            "url": canal.get("url", ""),
            "videos": videos,
            "titulos": [v["titulo"] for v in videos],
            "total_encontrados": len(videos),
            "fuente": data.get("fuente", "claude_web_search"),
            "fecha_extraccion": datetime.now().strftime("%Y-%m-%d"),
            "nota": "Visualizaciones no garantizadas — usar YouTube Data API para datos exactos"
        }
    except Exception as e:
        logger.warning(f"    ⚠️  Error para {nombre}: {e}")
        return {
            "canal": nombre, "handle": handle, "url": canal.get("url", ""),
            "videos": [], "titulos": [], "error": str(e),
            "fecha_extraccion": datetime.now().strftime("%Y-%m-%d")
        }


# ============================================
# ANÁLISIS GLOBAL DE PATRONES
# ============================================

def analyze_patterns(resultados: list[dict], client: anthropic.Anthropic) -> dict:
    """
    Pide a Claude que analice patrones estructurales y de rendimiento
    sobre todos los títulos + visualizaciones de la competencia.
    """
    all_videos = []
    for r in resultados:
        all_videos.extend(r.get("videos", []))

    if not all_videos:
        return {}

    logger.info("  🧠 Analizando patrones globales...")

    # Ordenar por visualizaciones para destacar los más exitosos
    top_videos = sorted(all_videos, key=lambda x: x.get("visualizaciones", 0), reverse=True)[:80]
    titulos_str = "\n".join(
        f"- [{v.get('visualizaciones', 0):,} views] {v['titulo']}"
        for v in top_videos
    )

    prompt = f"""Analiza estos {len(top_videos)} vídeos de canales cristianos exitosos en YouTube (ordenados por visualizaciones):

{titulos_str}

Extrae patrones accionables. JSON válido sin markdown:
{{
  "estructuras_frecuentes": [
    {{"patron": "descripción", "ejemplo": "título real", "frecuencia": "alta/media/baja", "views_promedio": 50000}}
  ],
  "palabras_clave_top": ["palabra1", "palabra2"],
  "emojis_mas_usados": ["🛑", "🔥"],
  "tematicas_dominantes": ["PROTECCIÓN", "ENEMIGOS"],
  "longitud_media_caracteres": 58,
  "titulos_mas_virales": ["título 1", "título 2", "título 3"],
  "insights_clave": ["insight 1 accionable basado en views", "insight 2"]
}}"""

    try:
        msg = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )
        text = "".join(b.text for b in msg.content if b.type == "text").strip()
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        return json.loads(text)
    except Exception as e:
        logger.warning(f"  ⚠️  Error en análisis de patrones: {e}")
        return {}


# ============================================
# MAIN
# ============================================

def main():
    parser = argparse.ArgumentParser(description="Obtiene referencias de canales YouTube con visualizaciones")
    parser.add_argument("--max-videos", type=int, default=10,
                        help="Máx. vídeos por canal (default: 10)")
    parser.add_argument("--solo-claude", action="store_true",
                        help="Fuerza uso de Claude web_search aunque haya API key")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("🎬 OBTENEDOR DE REFERENCIAS YOUTUBE")
    logger.info("=" * 60)

    config = load_config()
    canales = config.get("canales_referencia_youtube", [])
    if not canales:
        logger.error("❌ No hay canales en 'canales_referencia_youtube' del YAML")
        sys.exit(1)

    # Determinar modo de extracción
    youtube_api_key = os.getenv("YOUTUBE_API_KEY", "").strip()
    usar_api = bool(youtube_api_key) and youtube_api_key != "tu_clave_youtube_aqui" and not args.solo_claude

    anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
    if not anthropic_api_key:
        logger.error("❌ ANTHROPIC_API_KEY no encontrada en .env")
        sys.exit(1)
    claude_client = anthropic.Anthropic(api_key=anthropic_api_key)

    if usar_api:
        logger.info("✅ Modo: YouTube Data API v3 (títulos + visualizaciones exactas)")
    else:
        logger.warning("⚠️  Modo: Claude web_search (sin YOUTUBE_API_KEY válida)")
        logger.warning("   Para obtener visualizaciones exactas, añade YOUTUBE_API_KEY en .env")

    resultados = []
    logger.info(f"\n📺 Procesando {len(canales)} canales (máx. {args.max_videos} vídeos c/u)...\n")

    for i, canal in enumerate(canales):
        if usar_api:
            resultado = fetch_channel_data_via_api(canal, args.max_videos, youtube_api_key)
        else:
            resultado = fetch_channel_data_via_claude(canal, args.max_videos, claude_client)

        resultados.append(resultado)

        # Pausa entre canales para evitar rate limiting
        if i < len(canales) - 1:
            time.sleep(1 if usar_api else 2)

    # Resumen de visualizaciones
    total_videos = sum(r.get("total_encontrados", 0) for r in resultados)
    total_views = sum(
        v.get("visualizaciones", 0)
        for r in resultados
        for v in r.get("videos", [])
    )
    logger.info(f"\n📊 Total vídeos recopilados: {total_videos} | Views totales indexadas: {total_views:,}")

    # Análisis de patrones con Claude
    patrones = analyze_patterns(resultados, claude_client)

    # Top 10 vídeos por visualizaciones (para referencia rápida)
    todos_videos = [
        {"canal": r["canal"], **v}
        for r in resultados
        for v in r.get("videos", [])
    ]
    top_10 = sorted(todos_videos, key=lambda x: x.get("visualizaciones", 0), reverse=True)[:10]

    output = {
        "version": "2.0",
        "fecha_generacion": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "modo_extraccion": "youtube_data_api_v3" if usar_api else "claude_web_search",
        "total_canales": len(canales),
        "total_videos": total_videos,
        "canales": resultados,
        "top_10_por_visualizaciones": top_10,
        "patrones_globales": patrones,
        # Campo legacy para compatibilidad con generar_calendario.py
        "todos_los_titulos": [v["titulo"] for v in todos_videos]
    }

    with open(REFERENCIAS_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    logger.info(f"\n✅ Referencias guardadas en: {REFERENCIAS_JSON}")
    logger.info(f"   Canales: {len(resultados)} | Vídeos: {total_videos}")
    if top_10:
        logger.info("\n🏆 Top 3 vídeos de la competencia por visualizaciones:")
        for v in top_10[:3]:
            logger.info(f"   [{v.get('visualizaciones', 0):,}] {v['canal']} — {v['titulo'][:60]}")
    logger.info("\n▶️  Siguiente paso: generar_calendario.py — cargará estas referencias automáticamente")


if __name__ == "__main__":
    main()