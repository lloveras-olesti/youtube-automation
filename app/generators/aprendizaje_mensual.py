#!/usr/bin/env python3
"""
Sistema de Aprendizaje Acumulativo Mensual
==========================================
Analiza el rendimiento real del mes anterior, extrae métricas e insights,
y los acumula en config/insights_acumulados.json para que generar_calendario.py
los use como contexto en el siguiente mes.

Versión: 2.0
Cambios v2.0:
  - Rutas portables: funciona en Windows (local) y en Docker
    mediante la variable CANAL_RELI_ROOT en .env
  - Columnas actualizadas: TÍTULO, CTR, IMPRESIONES, VISUALIZACIONES, TEMATICA
  - TEMATICA ya no se analiza como señal de rendimiento de contenido,
    sino como señal de rotación visual de portada (informativo, sin recomendaciones)
  - Eliminado actualizar_catalogo_yaml: el catálogo ya no tiene distribucion_pct
  - Modelo actualizado a claude-sonnet-4-6

Flujo mensual recomendado:
  1. sincronizar_youtube.py   → actualiza historico_videos.csv con CTR/impresiones reales
  2. aprendizaje_mensual.py   → analiza métricas y genera insights_acumulados.json
  3. generar_calendario.py    → usa insights para el nuevo mes

Uso:
  python aprendizaje_mensual.py                  # mes anterior automático
  python aprendizaje_mensual.py --mes 2026-02    # mes específico
  python aprendizaje_mensual.py --dry-run        # analiza pero NO modifica archivos

Ubicación: C:\\docker\\projects\\canal-reli\\app\\generators\\aprendizaje_mensual.py
"""

import os
import sys
import json
import yaml
import logging
import argparse
import re
import anthropic
import pandas as pd
from datetime import datetime
from pathlib import Path
from collections import Counter
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

# ============================================
# RUTAS PORTABLES (Windows local + Docker)
# ============================================
# Carga .env primero para que CANAL_RELI_ROOT esté disponible
_script_dir = Path(__file__).parent
_env_candidates = [
    _script_dir.parent.parent.parent / ".env",   # Windows: canal-reli/.env
    _script_dir.parent.parent / ".env",           # Docker:  /app/../.env
    Path("/app/.env"),                             # Docker fallback
]
for _env in _env_candidates:
    if _env.exists():
        load_dotenv(_env)
        break

def _resolve_project_root() -> Path:
    """
    Determina PROJECT_ROOT de forma portable:
    - Si CANAL_RELI_ROOT está en .env → úsalo directamente
    - Si no → calcula desde __file__ (3 niveles para Windows, 2 para Docker)
    """
    env_root = os.getenv("CANAL_RELI_ROOT")
    if env_root:
        return Path(env_root)
    # Heurística: sube niveles hasta encontrar la carpeta config/
    for levels in [3, 2, 4]:
        candidate = Path(__file__)
        for _ in range(levels):
            candidate = candidate.parent
        if (candidate / "config").exists():
            return candidate
    # Fallback absoluto
    return Path(__file__).parent.parent.parent

PROJECT_ROOT  = _resolve_project_root()
CONFIG_DIR    = PROJECT_ROOT / "config"
DATA_DIR      = PROJECT_ROOT / "data"
RUNTIME_DIR   = Path(os.environ.get("RUNTIME_PATH", str(PROJECT_ROOT / "runtime")))

HISTORICO_CSV  = RUNTIME_DIR / "historico_videos.csv"
CALENDARIO_CSV = DATA_DIR / "input" / "calendario.csv"
CONFIG_YAML    = CONFIG_DIR / "config_calendario.yaml"
INSIGHTS_JSON  = RUNTIME_DIR / "insights_acumulados.json"
LOGS_DIR       = DATA_DIR / "logs" / "aprendizaje"

# ============================================
# LOGGING
# ============================================
LOGS_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            LOGS_DIR / f"aprendizaje_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.log",
            encoding="utf-8"
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================
# CARGA DE DATOS
# ============================================

def load_historico() -> pd.DataFrame:
    if not HISTORICO_CSV.exists():
        logger.error(f"❌ No se encuentra historico_videos.csv en: {HISTORICO_CSV}")
        logger.error("   Ejecuta primero sincronizar_youtube.py")
        sys.exit(1)

    df = pd.read_csv(HISTORICO_CSV, encoding="utf-8", dtype=str)

    # Convertir numéricas
    for col in ["CTR", "IMPRESIONES", "VISUALIZACIONES"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df_clean = df[df["CTR"].notna()].copy()
    logger.info(f"📂 Histórico: {len(df_clean)} vídeos con CTR válido")
    return df_clean


def load_calendario() -> pd.DataFrame | None:
    if not CALENDARIO_CSV.exists():
        logger.warning("⚠️  No se encuentra calendario.csv — sin datos de estructura/testing")
        return None
    return pd.read_csv(CALENDARIO_CSV, encoding="utf-8", dtype=str)


def load_config() -> dict:
    with open(CONFIG_YAML, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_insights_existentes() -> dict:
    if INSIGHTS_JSON.exists():
        with open(INSIGHTS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "version": "2.0",
        "fecha_creacion": datetime.now().strftime("%Y-%m-%d"),
        "ultima_actualizacion": "",
        "total_ciclos": 0,
        "recomendaciones_vigentes": [],
        "ciclos": []
    }


# ============================================
# FILTRAR MES
# ============================================

def filter_mes(
    df_hist: pd.DataFrame,
    df_cal: pd.DataFrame | None,
    mes_str: str
) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    """
    Filtra al mes indicado cruzando histórico y calendario por TÍTULO.
    Si no hay calendario disponible, usa el histórico completo.
    """
    if df_cal is not None and "FECHA" in df_cal.columns:
        df_cal_mes = df_cal[df_cal["FECHA"].astype(str).str.startswith(mes_str)].copy()
        if len(df_cal_mes) > 0 and "TÍTULO" in df_cal_mes.columns:
            titulos_mes = set(df_cal_mes["TÍTULO"].tolist())
            df_hist_mes = df_hist[df_hist["TÍTULO"].isin(titulos_mes)].copy()
            logger.info(f"📅 Vídeos en calendario del mes: {len(df_cal_mes)}")
            logger.info(f"📅 Cruzados con histórico: {len(df_hist_mes)}")
            return df_hist_mes, df_cal_mes

    logger.warning("⚠️  Sin calendario para el mes — usando histórico completo disponible")
    return df_hist.copy(), None


# ============================================
# MÉTRICAS
# ============================================

def calcular_metricas(
    df_hist: pd.DataFrame,
    df_cal: pd.DataFrame | None,
    insights_existentes: dict,
    mes_str: str
) -> dict:

    m = {}

    # Rendimiento general
    m["ctr_medio_mes"]         = round(float(df_hist["CTR"].mean()), 3)
    m["ctr_mediana_mes"]       = round(float(df_hist["CTR"].median()), 3)
    m["impresiones_total"]     = int(df_hist["IMPRESIONES"].sum()) if "IMPRESIONES" in df_hist else 0
    m["visualizaciones_total"] = int(df_hist["VISUALIZACIONES"].sum()) if "VISUALIZACIONES" in df_hist else 0
    m["n_videos_analizados"]   = len(df_hist)

    # Análisis por estructura y testing (requiere calendario con ESTRUCTURA_ID)
    if df_cal is not None and "ESTRUCTURA_ID" in df_cal.columns and "TÍTULO" in df_cal.columns:
        df_merged = df_hist.merge(
            df_cal[["TÍTULO", "ESTRUCTURA_ID", "ES_TESTING", "VARIACION_COMPETENCIA"]],
            on="TÍTULO",
            how="left"
        )

        ctr_por_est = {
            str(eid): round(float(grp["CTR"].mean()), 3)
            for eid, grp in df_merged.groupby("ESTRUCTURA_ID")
            if pd.notna(eid) and str(eid).strip()
        }
        m["ctr_por_estructura"]   = ctr_por_est
        m["mejor_estructura_mes"] = max(ctr_por_est, key=ctr_por_est.get) if ctr_por_est else None
        m["peor_estructura_mes"]  = min(ctr_por_est, key=ctr_por_est.get) if ctr_por_est else None

        # Testing vs estándar
        if "ES_TESTING" in df_merged.columns:
            testing_mask  = df_merged["ES_TESTING"].astype(str).str.lower() == "true"
            testing_ctr   = df_merged[testing_mask]["CTR"]
            standard_ctr  = df_merged[~testing_mask]["CTR"]
            m["ctr_testing_vs_standard"] = {
                "testing":  round(float(testing_ctr.mean()),  3) if len(testing_ctr)  > 0 else None,
                "standard": round(float(standard_ctr.mean()), 3) if len(standard_ctr) > 0 else None
            }
            m["testing_superaron_media"] = int(testing_ctr.gt(m["ctr_medio_mes"]).sum())
        else:
            m["ctr_testing_vs_standard"] = None
            m["testing_superaron_media"]  = None

        # Variaciones de competencia vs originales
        if "VARIACION_COMPETENCIA" in df_merged.columns:
            var_mask  = df_merged["VARIACION_COMPETENCIA"].astype(str).str.lower() == "true"
            var_ctr   = df_merged[var_mask]["CTR"]
            orig_ctr  = df_merged[~var_mask]["CTR"]
            m["ctr_variaciones_vs_originales"] = {
                "variaciones": round(float(var_ctr.mean()),  3) if len(var_ctr)  > 0 else None,
                "originales":  round(float(orig_ctr.mean()), 3) if len(orig_ctr) > 0 else None,
                "n_variaciones": int(var_mask.sum())
            }
        else:
            m["ctr_variaciones_vs_originales"] = None

    else:
        m["ctr_por_estructura"]            = {}
        m["mejor_estructura_mes"]          = None
        m["peor_estructura_mes"]           = None
        m["ctr_testing_vs_standard"]       = None
        m["testing_superaron_media"]       = None
        m["ctr_variaciones_vs_originales"] = None

    # TEMATICA — solo como información de rotación visual (sin recomendaciones de contenido)
    # La temática ya no es señal de rendimiento narrativo, solo de imagen de portada.
    if "TEMATICA" in df_hist.columns:
        ctr_por_tem = {
            str(tem): round(float(grp["CTR"].mean()), 3)
            for tem, grp in df_hist.groupby("TEMATICA")
            if pd.notna(tem) and str(tem).strip() and str(tem).upper() != "OTRO"
        }
        m["ctr_por_tematica_portada"] = ctr_por_tem  # informativo, no accionable en título
    else:
        m["ctr_por_tematica_portada"] = {}

    # Análisis de salmos
    if "SALMO" in df_hist.columns:
        df_salmo = df_hist[df_hist["SALMO"].notna() & (df_hist["SALMO"].astype(str).str.strip() != "")]
        if len(df_salmo) > 0:
            ctr_por_salmo = {
                str(s): round(float(grp["CTR"].mean()), 3)
                for s, grp in df_salmo.groupby("SALMO")
            }
            m["ctr_por_salmo"]    = ctr_por_salmo
            m["salmos_top_ctr"]   = sorted(ctr_por_salmo, key=ctr_por_salmo.get, reverse=True)[:5]
            m["salmos_bajo_ctr"]  = sorted(ctr_por_salmo, key=ctr_por_salmo.get)[:3]
        else:
            m["ctr_por_salmo"]   = {}
            m["salmos_top_ctr"]  = []
            m["salmos_bajo_ctr"] = []
    else:
        m["ctr_por_salmo"]   = {}
        m["salmos_top_ctr"]  = []
        m["salmos_bajo_ctr"] = []

    # Análisis de títulos top (p75 en adelante)
    p75       = df_hist["CTR"].quantile(0.75)
    df_top    = df_hist[df_hist["CTR"] >= p75]
    titulos_top = df_top["TÍTULO"].tolist() if "TÍTULO" in df_top.columns else []

    m["longitud_optima_real"]     = round(float(pd.Series([len(t) for t in titulos_top]).mean()), 1) if titulos_top else None
    m["emojis_mas_efectivos"]     = _top_emojis(titulos_top, 3)
    m["palabras_clave_ganadoras"] = _top_palabras(titulos_top, 10)
    m["titulos_top_5"]            = titulos_top[:5]

    # Tendencia vs mes anterior
    ciclos = insights_existentes.get("ciclos", [])
    if ciclos:
        ctr_prev = ciclos[-1].get("metricas_clave", {}).get("ctr_medio_mes")
        if ctr_prev:
            delta = m["ctr_medio_mes"] - ctr_prev
            if delta > 0.1:
                m["tendencia_ctr"] = f"subiendo (+{delta:.2f}%)"
            elif delta < -0.1:
                m["tendencia_ctr"] = f"bajando ({delta:.2f}%)"
            else:
                m["tendencia_ctr"] = f"estable ({delta:+.2f}%)"
        else:
            m["tendencia_ctr"] = "primer ciclo — sin referencia"
    else:
        m["tendencia_ctr"] = "primer ciclo — sin referencia"

    return m


# ============================================
# HELPERS ANÁLISIS DE TÍTULOS
# ============================================

def _top_emojis(titulos: list, n: int) -> list:
    pat = re.compile(
        "[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF"
        "\U00002702-\U000027B0\U000024C2-\U0001F251]+",
        flags=re.UNICODE
    )
    emojis = [e for t in titulos for e in pat.findall(t)]
    return [e for e, _ in Counter(emojis).most_common(n)]


def _top_palabras(titulos: list, n: int) -> list:
    stop = {
        "de","la","el","en","y","a","que","los","las","un","una","del",
        "con","para","por","es","su","se","lo","al","más","tu","mi","te",
        "me","no","si","todo","toda","todos","todas","este","esta","hay"
    }
    words = []
    for t in titulos:
        words.extend(
            w for w in re.sub(r"[^\w\s]", " ", t.lower()).split()
            if w not in stop and len(w) > 3
        )
    return [w for w, _ in Counter(words).most_common(n)]


# ============================================
# INSIGHTS NARRATIVOS CON CLAUDE
# ============================================

def generar_insights_narrativos(metricas: dict, config: dict, mes_str: str) -> tuple[list, list]:
    """
    Claude analiza las métricas y genera:
    - insights_narrativos: observaciones concretas con números
    - recomendaciones_vigentes: acciones para el próximo mes (se inyectan en el prompt del calendario)
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.warning("⚠️  ANTHROPIC_API_KEY no encontrada — omitiendo análisis narrativo")
        return [], []

    client = anthropic.Anthropic(api_key=api_key)

    # Preparar resumen de métricas para el prompt (sin datos de portada)
    metricas_prompt = {k: v for k, v in metricas.items()
                       if k != "ctr_por_tematica_portada"}

    prompt = f"""Eres un analista experto en optimización de contenido YouTube para canales de oraciones sobre Salmos en español.

Analiza las métricas del mes {mes_str} y genera conclusiones accionables para el próximo mes.

MÉTRICAS DEL MES:
{json.dumps(metricas_prompt, indent=2, ensure_ascii=False)}

CONTEXTO DEL CANAL:
- Canal de oraciones sobre Salmos con tono agresivo y directo
- Verbos de impacto: Destruye, Quema, Aplasta, Rompe, Devuelve...
- Sujetos: Enemigos, Maldiciones, Hechizos, Ataduras...
- Emojis de prefijo: 🛑 🔥 ⚡ ⚔️ 🩸
- Un 25% de los títulos son variaciones de la competencia (VARIACION_COMPETENCIA: true)
- Un 20% son slots de testing (ES_TESTING: true)

Genera:
1. insights_narrativos: 4-6 observaciones concretas con números reales de las métricas
   (ej: "EST-01 superó la media en 0.8% CTR — reforzar para el próximo mes")
2. recomendaciones_vigentes: 4-6 recomendaciones accionables y específicas para el próximo calendario
   - Estas recomendaciones se inyectan DIRECTAMENTE en el prompt de generar_calendario.py
   - Deben ser instrucciones claras y ejecutables, no descripciones genéricas
   - Ejemplos buenos: "Incluir el Salmo 91 al menos 2 veces en el mes siguiente"
                      "Priorizar verbos DESTRUYE y ROMPE — superaron la media en 1.2%"
                      "Reducir preguntas retóricas — EST-02 por debajo de la media"

JSON válido sin markdown:
{{
  "insights_narrativos": ["observación 1 con números", "observación 2 con números"],
  "recomendaciones_vigentes": ["instrucción accionable 1", "instrucción accionable 2"]
}}"""

    try:
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}]
        )
        text = "".join(b.text for b in msg.content if b.type == "text").strip()
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].strip()
        data = json.loads(text)
        return data.get("insights_narrativos", []), data.get("recomendaciones_vigentes", [])
    except Exception as e:
        logger.warning(f"⚠️  Error en análisis narrativo: {e}")
        return [], []


# ============================================
# GUARDAR INSIGHTS
# ============================================

def guardar_insights(
    metricas: dict,
    insights_narrativos: list,
    recomendaciones: list,
    ins_exist: dict,
    mes_str: str,
    dry_run: bool = False
) -> dict:

    nuevo_ciclo = {
        "mes": mes_str,
        "fecha_procesado": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "n_videos_analizados": metricas.get("n_videos_analizados", 0),
        "metricas_clave": metricas,
        "insights_narrativos": insights_narrativos,
        "recomendaciones_aplicadas": recomendaciones
    }

    # Reemplazar si ya existe ciclo de este mes
    ciclos = [c for c in ins_exist.get("ciclos", []) if c.get("mes") != mes_str]
    ciclos.append(nuevo_ciclo)

    ins_exist["ciclos"]                 = ciclos
    ins_exist["total_ciclos"]           = len(ciclos)
    ins_exist["ultima_actualizacion"]   = datetime.now().strftime("%Y-%m-%d")
    ins_exist["recomendaciones_vigentes"] = recomendaciones

    if not dry_run:
        with open(INSIGHTS_JSON, "w", encoding="utf-8") as f:
            json.dump(ins_exist, f, indent=2, ensure_ascii=False)
        logger.info(f"✅ insights_acumulados.json actualizado ({len(ciclos)} ciclos totales)")
    else:
        logger.info("🔍 [DRY RUN] — insights calculados pero NO guardados")

    return ins_exist


# ============================================
# MAIN
# ============================================

def main():
    parser = argparse.ArgumentParser(description="Aprendizaje mensual acumulativo del canal")
    parser.add_argument("--mes", type=str, default=None,
                        help="Mes a analizar YYYY-MM (default: mes anterior)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Analiza sin modificar archivos")
    args = parser.parse_args()

    mes_str = args.mes or (
        (datetime.now().replace(day=1) - relativedelta(months=1)).strftime("%Y-%m")
    )

    logger.info("=" * 60)
    logger.info(f"🧠 APRENDIZAJE MENSUAL — {mes_str}")
    logger.info(f"   PROJECT_ROOT: {PROJECT_ROOT}")
    if args.dry_run:
        logger.info("   [DRY RUN — no se modificarán archivos]")
    logger.info("=" * 60)

    logger.info("\n📊 Paso 1/5: Cargando datos...")
    df_hist   = load_historico()
    df_cal    = load_calendario()
    config    = load_config()
    ins_exist = load_insights_existentes()

    logger.info(f"\n📅 Paso 2/5: Filtrando mes {mes_str}...")
    df_hist_mes, df_cal_mes = filter_mes(df_hist, df_cal, mes_str)

    if len(df_hist_mes) == 0:
        logger.warning(f"⚠️  Sin vídeos del mes {mes_str} en el histórico")
        logger.warning("   Ejecuta primero sincronizar_youtube.py para ese mes")
        sys.exit(0)

    logger.info(f"\n📈 Paso 3/5: Calculando métricas ({len(df_hist_mes)} vídeos)...")
    metricas = calcular_metricas(df_hist_mes, df_cal_mes, ins_exist, mes_str)
    logger.info(f"   CTR medio:    {metricas['ctr_medio_mes']}%")
    logger.info(f"   CTR mediana:  {metricas['ctr_mediana_mes']}%")
    logger.info(f"   Impresiones:  {metricas['impresiones_total']:,}")
    logger.info(f"   Tendencia:    {metricas['tendencia_ctr']}")
    if metricas.get("mejor_estructura_mes"):
        logger.info(f"   Mejor EST:    {metricas['mejor_estructura_mes']}")
    if metricas.get("salmos_top_ctr"):
        logger.info(f"   Salmos top:   {metricas['salmos_top_ctr']}")
    if metricas.get("ctr_variaciones_vs_originales"):
        v = metricas["ctr_variaciones_vs_originales"]
        logger.info(f"   Variaciones competencia: {v.get('variaciones')}% vs originales: {v.get('originales')}%")

    logger.info("\n🤖 Paso 4/5: Generando insights narrativos con Claude...")
    insights_narrativos, recomendaciones = generar_insights_narrativos(metricas, config, mes_str)
    if insights_narrativos:
        for ins in insights_narrativos[:3]:
            logger.info(f"   • {ins[:90]}")
    if recomendaciones:
        logger.info("   Recomendaciones generadas:")
        for rec in recomendaciones[:3]:
            logger.info(f"   ⭐ {rec[:80]}")

    logger.info("\n💾 Paso 5/5: Guardando insights_acumulados.json...")
    guardar_insights(
        metricas, insights_narrativos, recomendaciones,
        ins_exist, mes_str, dry_run=args.dry_run
    )

    logger.info("\n" + "=" * 60)
    logger.info("✅ APRENDIZAJE MENSUAL COMPLETADO")
    logger.info("=" * 60)
    if not args.dry_run:
        logger.info(f"   insights_acumulados.json → {INSIGHTS_JSON}")
        logger.info("▶️  Siguiente paso: generar_calendario.py")
    else:
        logger.info("   [DRY RUN] No se han modificado archivos")


if __name__ == "__main__":
    main()