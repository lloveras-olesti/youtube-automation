#!/usr/bin/env python3
"""
Generador de Calendario de Videos - Canal Cristiano
====================================================
Genera 30 vídeos optimizados usando Claude API con análisis del histórico,
referencias de competencia e insights acumulados.

Versión: 3.1
Cambios v3.1:
  - Catálogo de estructuras pasa a ser ORIENTATIVO (no distribución forzada)
  - 25% de títulos son variaciones directas de títulos top de la competencia
  - Señal explícita de variedad: el modelo recibe los últimos 10 títulos propios
    para evitar repetición de estructura detectada por el algoritmo
  - Sección de competencia ampliada con top vídeos por visualizaciones

Ubicación: C:\\docker\\projects\\canal-reli\\app\\generators\\generar_calendario.py
"""

import os
import sys
import json
import yaml
import logging
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import anthropic
import pandas as pd
from dotenv import load_dotenv

# ============================================
# RUTAS
# ============================================
PROJECT_ROOT     = Path(__file__).parent.parent.parent
CONFIG_DIR       = PROJECT_ROOT / "config"
DATA_DIR         = PROJECT_ROOT / "data"

CONFIG_FILE      = CONFIG_DIR / "config_calendario.yaml"
HISTORICO_CSV    = CONFIG_DIR / "historico_videos.csv"
OUTPUT_CSV       = DATA_DIR / "input" / "calendario.csv"
REFERENCIAS_JSON = CONFIG_DIR / "referencias_canales.json"
INSIGHTS_JSON    = CONFIG_DIR / "insights_acumulados.json"
LOGS_DIR         = DATA_DIR / "logs" / "calendario"

load_dotenv(PROJECT_ROOT / ".env")


# ============================================
# LOGGING
# ============================================

def setup_logging():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
    log_file = LOGS_DIR / f"{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    logger.info(f"📝 Log: {log_file}")
    return logger

logger = None


# ============================================
# CARGA DE CONFIG
# ============================================

def load_config() -> dict:
    if not CONFIG_FILE.exists():
        print(f"❌ No se encuentra config: {CONFIG_FILE}")
        sys.exit(1)
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ============================================
# CARGA DE DATOS HISTÓRICOS
# ============================================

def load_historical_data(config: dict) -> pd.DataFrame:
    if not HISTORICO_CSV.exists():
        logger.error(f"❌ No se encuentra histórico: {HISTORICO_CSV}")
        sys.exit(1)

    df = pd.read_csv(HISTORICO_CSV, encoding="utf-8", dtype=str)
    logger.info(f"📂 Histórico cargado: {len(df)} filas")

    for col in ["CTR", "IMPRESIONES", "VISUALIZACIONES"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df_clean = df[df["CTR"].notna()].copy()
    logger.info(f"✅ {len(df_clean)} vídeos con CTR válido | CTR medio: {df_clean['CTR'].mean():.2f}%")
    return df_clean


def preprocess_historical_data(df: pd.DataFrame, config: dict) -> tuple[str, str]:
    """
    Top performers → CSV completo al prompt.
    Bottom performers → resumen estadístico por salmo (ahorra tokens).
    TEMATICA excluida del análisis de rendimiento.
    """
    prep      = config.get("preprocesado_historico", {})
    percentil = prep.get("percentil_top", 60)
    max_filas = prep.get("max_filas_top_a_enviar", 80)

    umbral    = df["CTR"].quantile(percentil / 100)
    df_top    = df[df["CTR"] >= umbral].sort_values("CTR", ascending=False).head(max_filas)
    df_bottom = df[df["CTR"] <  umbral]

    cols    = ["FECHA", "TÍTULO", "CTR", "IMPRESIONES", "SALMO", "CONTENIDO"]
    cols_ok = [c for c in cols if c in df_top.columns]
    csv_top = df_top[cols_ok].to_csv(index=False)

    resumen = [f"\n# RESUMEN ESTADÍSTICO (vídeos bajo percentil {percentil})"]
    if "SALMO" in df_bottom.columns:
        por_salmo = df_bottom.groupby("SALMO")["CTR"].agg(["mean", "count"]).round(2)
        for salmo, row in por_salmo.iterrows():
            resumen.append(f"  Salmo {salmo}: CTR medio {row['mean']}% ({int(row['count'])} vídeos)")

    return csv_top, "\n".join(resumen)


# ============================================
# SEÑAL DE VARIEDAD EN TÍTULOS
# ============================================

def get_ultimos_titulos(df: pd.DataFrame, n: int = 10) -> str:
    """
    Devuelve los últimos N títulos propios publicados.
    Se inyectan en el prompt para que Claude evite repetir estructuras recientes.
    """
    recientes = df.sort_values("FECHA", ascending=False).head(n)
    titulos = recientes["TÍTULO"].dropna().tolist()
    if not titulos:
        return ""
    lines = ["\n# ÚLTIMOS TÍTULOS PUBLICADOS (evitar estructuras repetidas)"]
    lines.append("El algoritmo de YouTube puede detectar patrones. NO repitas estas estructuras:")
    for t in titulos:
        lines.append(f"  — {t}")
    return "\n".join(lines)


# ============================================
# TEMÁTICAS — pool manual + señal de rotación
# ============================================

def get_tematica_rotation(df: pd.DataFrame, config: dict) -> tuple[list[str], str]:
    pool = config.get("tematicas_portada", [])
    if not pool:
        pool = [t for t in df["TEMATICA"].dropna().unique().tolist() if str(t).upper() != "OTRO"]
        logger.warning("⚠️  'tematicas_portada' no en config — usando valores del histórico")

    logger.info(f"🖼️  Pool de temáticas ({len(pool)}): {', '.join(pool)}")

    recientes     = df.sort_values("FECHA", ascending=False).head(30)
    recientes_vals = recientes["TEMATICA"].dropna().tolist()[:15]
    conteo        = Counter(recientes_vals)

    uso_str      = " | ".join(f"{t}: {n}x" for t, n in sorted(conteo.items(), key=lambda x: -x[1])) or "Sin datos"
    ultimas_5    = " → ".join(recientes_vals[:5]) if recientes_vals else "Sin datos"

    seccion = f"""
# TEMÁTICAS DE PORTADA — rotación obligatoria

Pool permitido (ÚNICAMENTE estos valores):
{json.dumps(pool, ensure_ascii=False)}

Uso reciente (últimas 30 entradas del histórico):
  Frecuencia acumulada: {uso_str}
  Últimas 5 usadas:     {ultimas_5}

Reglas:
- Distribuye equilibradamente. Ninguna supera el 25% del total (~7 de 30).
- Prioriza las de MENOR frecuencia reciente.
- NO repitas la misma en 2 vídeos consecutivos.
- NUNCA uses valores fuera del pool.
"""
    return pool, seccion


# ============================================
# VARIACIONES DE COMPETENCIA
# ============================================

def get_competencia_variaciones(refs: dict | None, config: dict) -> tuple[list[str], str]:
    """
    Extrae los títulos más virales de la competencia (por visualizaciones)
    y construye la sección del prompt para que Claude genere variaciones directas.
    Retorna (lista_de_titulos_fuente, seccion_prompt).
    """
    var_cfg = config.get("variaciones_competencia", {})
    if not var_cfg.get("habilitado", True) or not refs:
        return [], ""

    pct    = var_cfg.get("porcentaje", 25)
    dias   = config.get("dias_a_generar", 30)
    n_var  = max(1, round(dias * pct / 100))

    # Obtener top vídeos de la competencia ordenados por visualizaciones
    todos_videos = []
    for canal in refs.get("canales", []):
        for v in canal.get("videos", []):
            todos_videos.append({
                "canal":          canal.get("canal", "?"),
                "titulo":         v.get("titulo", ""),
                "visualizaciones": v.get("visualizaciones", 0)
            })

    # Fallback: si no hay visualizaciones, usar todos_los_titulos del JSON legacy
    if not todos_videos:
        titulos_legacy = refs.get("todos_los_titulos", [])
        todos_videos = [{"canal": "?", "titulo": t, "visualizaciones": 0} for t in titulos_legacy]

    if not todos_videos:
        return [], ""

    top_fuente = sorted(todos_videos, key=lambda x: x.get("visualizaciones", 0), reverse=True)[:30]

    logger.info(f"🎯 Variaciones de competencia: {n_var} títulos basados en top {len(top_fuente)} vídeos externos")

    titulos_fuente_str = "\n".join(
        f"  [{v['visualizaciones']:,} views] {v['canal']} → \"{v['titulo']}\""
        for v in top_fuente
    )

    seccion = f"""
# VARIACIONES DE COMPETENCIA ({n_var} títulos obligatorios)

Exactamente {n_var} de los {dias} títulos deben ser VARIACIONES DIRECTAS de títulos exitosos
de la competencia. Una variación directa significa:
- Mantener el ÁNGULO NARRATIVO del original (lo que lo hace clickeable)
- Cambiar el salmo, los personajes concretos o el sujeto
- Añadir el estilo agresivo y los emojis del canal
- El resultado debe sonar diferente pero aprovechar la misma fórmula psicológica

Ejemplo de variación:
  Original:  "🔥Oración Poderosa para Destruir Planes del Enemigo"
  Variación: "⚡SALMO 35: Aplasta Los Planes Malignos Contra Tu Familia HOY"

Títulos fuente de la competencia (ordenados por visualizaciones):
{titulos_fuente_str}

Marca cada variación con VARIACION_COMPETENCIA: true en el JSON.
Distribuye los {n_var} vídeos de variación a lo largo del mes (no todos juntos).
"""
    titulos_solo = [v["titulo"] for v in top_fuente]
    return titulos_solo, seccion


# ============================================
# SECCIONES DEL PROMPT
# ============================================

def _seccion_referencias_generales(refs: dict | None) -> str:
    """Sección con patrones globales de la competencia (distinta de las variaciones directas)."""
    if not refs:
        return ""

    patrones = refs.get("patrones_globales", {})
    if not patrones:
        return ""

    lines = ["\n# PATRONES GLOBALES DE LA COMPETENCIA"]
    for ins in patrones.get("insights_clave", [])[:4]:
        lines.append(f"  • {ins}")
    for t in patrones.get("titulos_mas_virales", [])[:5]:
        lines.append(f"  — {t}")

    return "\n".join(lines)


def _seccion_insights(ins: dict | None) -> str:
    if not ins or not ins.get("ciclos"):
        return ""

    ultimo = ins["ciclos"][-1]
    m      = ultimo.get("metricas_clave", {})
    recs   = ins.get("recomendaciones_vigentes", [])

    lines = [f"\n# APRENDIZAJE ACUMULADO ({ins.get('total_ciclos', 1)} ciclos)"]
    if m.get("ctr_medio_mes"):
        lines.append(f"  CTR medio: {m['ctr_medio_mes']}%  |  Tendencia: {m.get('tendencia_ctr', '?')}")
    if recs:
        lines.append("\nRecomendaciones vigentes:")
        for r in recs:
            lines.append(f"  ⭐ {r}")

    return "\n".join(lines)


def _seccion_catalogo(config: dict) -> str:
    """
    El catálogo pasa a ser orientativo: Claude lo usa como inspiración
    para generar variedad, sin estar obligado a respetar distribuciones exactas.
    """
    cat = config.get("catalogo_estructuras_titulo", [])
    if not cat:
        return ""

    lines = [
        "\n# ESTILOS DE TÍTULO — referencia orientativa",
        "Usa estos estilos como INSPIRACIÓN para generar variedad.",
        "NO estás obligado a respetar porcentajes exactos.",
        "El objetivo es que el conjunto de 30 títulos no tenga un patrón repetitivo",
        "que el algoritmo de YouTube pueda identificar.\n"
    ]
    for e in cat:
        lines.append(f"  [{e['id']}] {e['nombre']}")
        lines.append(f"    Patrón:  {e['patron']}")
        lines.append(f"    Ejemplo: \"{e['ejemplo']}\"\n")

    lines.append("Incluye ESTRUCTURA_ID en cada entrada (ej: \"EST-03\"). Testing → \"TESTING\".")
    return "\n".join(lines)


def _seccion_testing(config: dict, dias: int) -> str:
    slots = config.get("slots_testing", {})
    if not slots.get("habilitado"):
        return ""

    pct    = slots.get("porcentaje", 20)
    n_test = max(1, round(dias * pct / 100))
    return (
        f"\n# SLOTS DE TESTING ({n_test} de {dias} vídeos)\n"
        f"Marca {n_test} entradas con ES_TESTING: true y ESTRUCTURA_ID: \"TESTING\".\n"
        f"Estos vídeos exploran títulos y portadas fuera del estilo habitual del canal.\n"
        f"Distribúyelos a lo largo del mes, no todos juntos.\n"
        f"{slots.get('descripcion_testing', '')}"
    )


# ============================================
# FECHA INICIAL
# ============================================

def get_fecha_inicial(config: dict) -> str:
    if config.get("fecha_inicial", "auto") != "auto":
        return config["fecha_inicial"]
    if OUTPUT_CSV.exists():
        try:
            df = pd.read_csv(OUTPUT_CSV, encoding="utf-8")
            if len(df) > 0 and "FECHA" in df.columns:
                # Cambiado para usar la primera fecha disponible en el calendario actual
                # Esto permite mantener el ritmo constante en ciclos de generación.
                primera = pd.to_datetime(df["FECHA"].min())
                fi = primera.strftime("%Y-%m-%d")
                logger.info(f"📅 Reiniciando ciclo desde la primera fecha del calendario: {fi}")
                return fi
        except Exception:
            pass
    # Si no hay calendario previo o hay error, iniciar desde mañana
    fi = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"📅 Iniciando desde (mañana): {fi}")
    return fi


# ============================================
# CONSTRUCCIÓN DEL PROMPT
# ============================================

def create_analysis_prompt(df_hist: pd.DataFrame, config: dict,
                            refs: dict | None, ins: dict | None) -> str:

    csv_top, resumen_bottom       = preprocess_historical_data(df_hist, config)
    pool_tematicas, sec_tematicas = get_tematica_rotation(df_hist, config)
    titulos_fuente, sec_variaciones = get_competencia_variaciones(refs, config)
    ultimos_titulos_str           = get_ultimos_titulos(df_hist, n=10)
    fecha_ini = get_fecha_inicial(config)
    dias      = config["dias_a_generar"]
    pref      = config.get("preferencias_titulos", {})

    slots_cfg  = config.get("slots_testing", {})
    testing_on = slots_cfg.get("habilitado", False)
    var_cfg    = config.get("variaciones_competencia", {})
    var_on     = var_cfg.get("habilitado", True) and bool(titulos_fuente)

    testing_field    = '"ES_TESTING": false,' if testing_on else ""
    estructura_field = '"ESTRUCTURA_ID": "EST-01",' if config.get("catalogo_estructuras_titulo") else ""
    variacion_field  = '"VARIACION_COMPETENCIA": false,' if var_on else ""

    prompt = f"""Eres un experto en optimización de contenido para YouTube de temática cristiana.

# MISIÓN
Analizar el histórico de {len(df_hist)} vídeos y generar un calendario de {dias} vídeos
optimizado para maximizar CTR e impresiones.

════════════════════════════════════════════════════════════
# DATOS HISTÓRICOS — TOP PERFORMERS
════════════════════════════════════════════════════════════
```csv
{csv_top}
```
{resumen_bottom}

{ultimos_titulos_str}
{_seccion_referencias_generales(refs)}
{_seccion_insights(ins)}

════════════════════════════════════════════════════════════
# REGLAS DE SALMOS
════════════════════════════════════════════════════════════
Alta frecuencia (OBLIGATORIO incluirlos):
{json.dumps(config.get("salmos_alta_frecuencia", []), indent=2, ensure_ascii=False)}

Mínimo {config.get("dias_minimos_repeticion", 30)} días entre repeticiones del mismo salmo.

{sec_tematicas}
{sec_variaciones}
{_seccion_catalogo(config)}
{_seccion_testing(config, dias)}

════════════════════════════════════════════════════════════
# TÍTULO — TONO Y ESTILO
════════════════════════════════════════════════════════════
Tono: AGRESIVO, DIRECTO, PODEROSO. Sin corrección política innecesaria.

Verbos de impacto:
  Destruye · Quema · Aplasta · Aniquila · Rompe · Quiebra · Devuelve
  Derrumba · Expulsa · Frustra · Revienta · Paraliza · Confunde · Ata

Sujetos concretos:
  Enemigos · Maldiciones · Hechizos · Brujería · Ataduras · Cadenas
  Planes Malignos · Envidia · Falsos Amigos · Espíritus Malignos · Mal de Ojo

Urgencia:
  Al Instante · En 24 Horas · Para Siempre · Hoy · En 7 Días · Esta Noche

Emojis de prefijo (rotar, sin repetir el mismo dos días seguidos):
  🛑 🔥 ⚡ ⚔️ 🩸 💥 ☠️

Longitud: {pref.get("longitud_min", 55)}-{pref.get("longitud_max", 90)} caracteres.

VARIEDAD CRÍTICA: Ningún título debe compartir estructura idéntica con otro del mismo calendario.
El algoritmo de YouTube detecta patrones en títulos de un mismo canal.
Usa los estilos orientativos como punto de partida, no como plantilla fija.

NO usar: "Reflexión sobre...", "Meditación de...", lenguaje pasivo o suave.

════════════════════════════════════════════════════════════
# CONTENIDO — TEXTO DE PORTADA (5 LÍNEAS)
════════════════════════════════════════════════════════════
CONTENIDO = texto de la imagen de portada. Exactamente 5 líneas (array JSON).
TODO EN MAYÚSCULAS. Sin puntuación. Máximo 20 caracteres por línea.

## PATRÓN A — base (60%):
  L1: SALMO + número   →  "SALMO 91"
  L2: Verbo poderoso   →  "DESTRUYE" / "QUEMA" / "APLASTA"
  L3: Sujeto           →  "TUS ENEMIGOS" / "LAS MALDICIONES"
  L4: Complemento      →  "OCULTOS" / "AL REMITENTE" / "SOBRE TU VIDA"
  L5: Cierre           →  "EN 24 HORAS" / "PARA SIEMPRE" / "X7 MÁS FUERTE"

  Ejemplos:
    ["SALMO 59", "DESTRUYE", "TUS ENEMIGOS", "OCULTOS", "EN 24 HORAS"]
    ["SALMO 35", "QUEMA", "HECHIZOS", "CONTRA TU HOGAR", "AL INSTANTE"]
    ["SALMO 91", "DEVUELVE", "TODO HECHIZO", "AL REMITENTE", "X7 MÁS FUERTE"]
    ["SALMO 5", "QUIEN SE LEVANTE", "CONTRA TI", "CAE", "HOY"]

## PATRÓN B — primera frase en dos líneas (20%):
  L1+L2: Frase inicial dividida  →  "EL SALMO" + "PROHIBIDO"
  L3-L5: Desarrollo y cierre

  Ejemplos:
    ["EL SALMO", "PROHIBIDO", "QUE DESTRUYE", "A TUS ENEMIGOS", "AL INSTANTE"]
    ["EL SALMO", "QUE DIOS NOS DIO", "PARA VENCER", "EL MIEDO", "Y LA ANSIEDAD"]
    ["LA ORACIÓN", "MÁS PODEROSA", "ELIMINA HECHIZOS", "MALDICIONES", "Y BRUJERÍA"]

## PATRÓN C — libre (20%, preferido en slots testing):
  5 líneas de construcción libre. Tono agresivo. Sin seguir A ni B.

════════════════════════════════════════════════════════════
# ANÁLISIS REQUERIDO
════════════════════════════════════════════════════════════
Antes de generar el calendario, identifica:
1. Patrones de títulos con mayor CTR: estructura, emojis, verbos, longitud
2. Rendimiento por salmo: CTR promedio e impresiones
3. Correlación entre CONTENIDO de portada y CTR (si hay datos suficientes)
4. Qué ángulos narrativos generan más clicks en el canal

════════════════════════════════════════════════════════════
# GENERACIÓN DEL CALENDARIO
════════════════════════════════════════════════════════════
Genera exactamente {dias} entradas desde {fecha_ini}.

Campos:
  FECHA                — YYYY-MM-DD consecutivos
  TÍTULO               — Título optimizado
  SALMO                — Número
  TEXTO                — Uno de {config.get("textos_posibles", ["formal", "cercano", "reflexivo"])}
  INTRO                — Uno de {config.get("intros_posibles", ["intro-rapida", "intro-contextual"])}
  TEMATICA             — Uno de {json.dumps(pool_tematicas, ensure_ascii=False)}
  CONTENIDO            — Array de exactamente 5 strings (MAYÚSCULAS, máx. 20 chars)
  ES_TESTING           — true/false
  ESTRUCTURA_ID        — ID del catálogo o "TESTING"
  VARIACION_COMPETENCIA — true si es variación de la competencia, false si no

════════════════════════════════════════════════════════════
# FORMATO DE SALIDA — JSON PURO SIN MARKDOWN
════════════════════════════════════════════════════════════
{{
  "analisis": {{
    "patrones_titulo_exitosos": ["patrón 1", "patrón 2"],
    "salmos_mejor_ctr": [91, 23, 35],
    "verbos_mas_efectivos": ["DESTRUYE", "ROMPE"],
    "insights_clave": ["insight 1", "insight 2"],
    "n_variaciones_competencia": 0,
    "distribucion_tematicas_aplicada": {{}},
    "distribucion_estructuras_aplicada": {{}}
  }},
  "calendario": [
    {{
      "FECHA": "{fecha_ini}",
      "TÍTULO": "🛑Título poderoso aquí",
      "SALMO": "91",
      "TEXTO": "formal",
      "INTRO": "intro-rapida",
      "TEMATICA": "LEON",
      "CONTENIDO": ["SALMO 91", "DESTRUYE", "TUS ENEMIGOS", "OCULTOS", "EN 24 HORAS"],
      {testing_field}
      {estructura_field}
      {variacion_field}
    }}
  ]
}}

CRÍTICO:
- Exactamente {dias} entradas.
- TEXTO e INTRO: solo valores exactos de los listados.
- TEMATICA: solo valores del pool permitido, distribución equilibrada.
- CONTENIDO: array de exactamente 5 strings, MAYÚSCULAS, máx. 20 chars por string.
- VARIACION_COMPETENCIA: true en exactamente las entradas que son variaciones directas.
- JSON puro — cero markdown, cero texto fuera del JSON.
"""
    return prompt


# ============================================
# LLAMADA A CLAUDE API
# ============================================

def call_claude_api(prompt: str, config: dict) -> str:
    logger.info("🤖 Conectando con Claude API...")
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("❌ ANTHROPIC_API_KEY no encontrada en .env")
        sys.exit(1)

    client  = anthropic.Anthropic(api_key=api_key)
    modelo  = config.get("modelo", "claude-sonnet-4-6")
    max_tok = config.get("max_tokens", 16000)

    tools = None
    if config.get("usar_benchmark_youtube", False):
        tools = [{"type": "web_search_20250305", "name": "web_search"}]
        logger.info("🔍 Benchmarking YouTube activado")

    try:
        logger.info(f"⏳ Generando calendario con {modelo} (30-60s)...")
        msg = client.messages.create(
            model=modelo,
            max_tokens=max_tok,
            messages=[{"role": "user", "content": prompt}],
            **({"tools": tools} if tools else {})
        )
        resp = "".join(b.text for b in msg.content if b.type == "text")
        logger.info("✅ Respuesta recibida")
        return resp
    except Exception as e:
        logger.error(f"❌ Error API: {e}")
        sys.exit(1)


# ============================================
# PARSEO DE RESPUESTA
# ============================================

def parse_response(response: str) -> dict:
    logger.info("📝 Parseando respuesta...")
    clean = response.strip()
    if "```json" in clean:
        clean = clean.split("```json")[1].split("```")[0]
    elif "```" in clean:
        parts = clean.split("```")
        if len(parts) >= 3:
            clean = parts[1]
    clean = clean.strip()

    try:
        data = json.loads(clean)
    except json.JSONDecodeError as e:
        logger.error(f"❌ Error JSON: {e}\nPrimeros 500 chars:\n{clean[:500]}")
        sys.exit(1)

    if "analisis" not in data or "calendario" not in data:
        logger.error(f"❌ Estructura incorrecta. Keys: {list(data.keys())}")
        sys.exit(1)

    logger.info(f"✅ {len(data['calendario'])} entradas parseadas")
    for ins in data["analisis"].get("insights_clave", [])[:3]:
        logger.info(f"   💡 {ins}")
    return data


# ============================================
# GUARDADO DEL CALENDARIO
# ============================================

def save_calendar(calendario: list, config: dict) -> pd.DataFrame:
    logger.info(f"💾 Guardando en: {OUTPUT_CSV}")
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(calendario)

    for col in ["FECHA", "TÍTULO", "SALMO", "TEXTO", "INTRO"]:
        if col not in df.columns:
            logger.error(f"❌ Columna faltante: {col}")
            sys.exit(1)

    defaults = {
        "TEMATICA": "", "CONTENIDO": "",
        "ES_TESTING": False, "ESTRUCTURA_ID": "",
        "VARIACION_COMPETENCIA": False
    }
    for col, val in defaults.items():
        if col not in df.columns:
            df[col] = val

    df["CONTENIDO"] = df["CONTENIDO"].apply(
        lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else x
    )

    for col in ["DESCRIPCION", "ETIQUETAS", "TAMAÑOS", "COLORES"]:
        df[col] = ""

    cols_output = [
        "FECHA", "TÍTULO", "SALMO", "TEXTO", "INTRO",
        "TEMATICA", "CONTENIDO",
        "ES_TESTING", "ESTRUCTURA_ID", "VARIACION_COMPETENCIA",
        "DESCRIPCION", "ETIQUETAS", "TAMAÑOS", "COLORES"
    ]
    df = df[[c for c in cols_output if c in df.columns]]
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    logger.info("✅ calendario.csv guardado")

    n_test = int(df["ES_TESTING"].astype(str).str.lower().eq("true").sum())
    n_var  = int(df["VARIACION_COMPETENCIA"].astype(str).str.lower().eq("true").sum())
    if n_test:
        logger.info(f"🧪 Slots de testing: {n_test}/{len(df)}")
    if n_var:
        logger.info(f"🎯 Variaciones de competencia: {n_var}/{len(df)}")

    return df


# ============================================
# GUARDAR LOG DE ANÁLISIS
# ============================================

def save_analysis(analisis: dict):
    ts   = datetime.now().strftime("%Y-%m-%d_%H-%M")
    path = LOGS_DIR / f"analisis_{ts}.json"
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(analisis, f, indent=2, ensure_ascii=False)
        logger.info(f"📊 Análisis guardado: {path.name}")
    except Exception as e:
        logger.warning(f"⚠️  Error guardando análisis: {e}")


# ============================================
# CARGA DE ARCHIVOS AUXILIARES
# ============================================

def load_referencias() -> dict | None:
    if not REFERENCIAS_JSON.exists():
        logger.info("ℹ️  referencias_canales.json no encontrado — ejecuta obtener_referencias.py")
        return None
    try:
        with open(REFERENCIAS_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        total = data.get("total_videos", data.get("total_titulos", 0))
        logger.info(f"📺 Referencias: {total} vídeos | {data.get('fecha_generacion', '?')}")
        return data
    except Exception as e:
        logger.warning(f"⚠️  Error cargando referencias: {e}")
        return None


def load_insights() -> dict | None:
    if not INSIGHTS_JSON.exists():
        return None
    try:
        with open(INSIGHTS_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"🧠 Insights acumulados: {data.get('total_ciclos', 0)} ciclos")
        return data
    except Exception as e:
        logger.warning(f"⚠️  Error cargando insights: {e}")
        return None


# ============================================
# MAIN
# ============================================

def main():
    global logger
    logger = setup_logging()

    logger.info("=" * 70)
    logger.info("🚀 GENERADOR DE CALENDARIO v3.1 — CANAL CRISTIANO")
    logger.info("=" * 70)
    logger.info(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        logger.info("\n📋 Paso 1/8: Cargando configuración...")
        config = load_config()

        logger.info("\n📊 Paso 2/8: Cargando datos históricos...")
        df_hist = load_historical_data(config)

        logger.info("\n📺 Paso 3/8: Cargando referencias de competencia...")
        refs = load_referencias()

        logger.info("\n🧠 Paso 4/8: Cargando insights acumulados...")
        ins = load_insights()

        logger.info("\n✍️  Paso 5/8: Construyendo prompt...")
        prompt = create_analysis_prompt(df_hist, config, refs, ins)
        logger.info(f"✅ Prompt listo: {len(prompt):,} caracteres")

        logger.info("\n🤖 Paso 6/8: Consultando Claude API...")
        response = call_claude_api(prompt, config)

        logger.info("\n📝 Paso 7/8: Procesando respuesta...")
        data = parse_response(response)

        logger.info("\n💾 Paso 8/8: Guardando resultados...")
        df_result = save_calendar(data["calendario"], config)
        save_analysis(data["analisis"])

        logger.info("\n" + "=" * 70)
        logger.info("✅ COMPLETADO")
        logger.info("=" * 70)
        logger.info(f"📅 Vídeos: {len(df_result)} ({df_result['FECHA'].iloc[0]} → {df_result['FECHA'].iloc[-1]})")
        logger.info(f"📖 Salmos únicos: {df_result['SALMO'].nunique()}")

        if "TEMATICA" in df_result.columns:
            logger.info(f"🖼️  Distribución temáticas: {df_result['TEMATICA'].value_counts().to_dict()}")

        if "ESTRUCTURA_ID" in df_result.columns:
            logger.info("🏗️  Estructuras:")
            for est, cnt in df_result["ESTRUCTURA_ID"].value_counts().items():
                logger.info(f"   {est}: {cnt}")

        logger.info(f"\n🎬 Primeros 5 vídeos:")
        for _, row in df_result.head(5).iterrows():
            flags = ""
            if str(row.get("ES_TESTING", "")).lower() == "true":     flags += " 🧪"
            if str(row.get("VARIACION_COMPETENCIA", "")).lower() == "true": flags += " 🎯"
            logger.info(f"   {row['FECHA']} [{row.get('TEMATICA','')}] Salmo {row['SALMO']}: {row['TÍTULO']}{flags}")

        return 0

    except KeyboardInterrupt:
        logger.warning("⚠️  Interrumpido")
        return 1
    except Exception as e:
        logger.error(f"❌ ERROR FATAL: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())