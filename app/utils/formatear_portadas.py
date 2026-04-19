#!/usr/bin/env python3
"""
Formateador de Portadas — Canal Reli
=====================================
Lee las filas de calendario.csv que tienen CONTENIDO relleno pero
TAMAÑOS y COLORES vacíos, y usa Claude para asignar a cada línea
el tamaño de fuente y el color adecuados para la portada.

Los tamaños y colores se calculan bajo constraints calibrados
con Anton + generar_portada.py:
  - Zona de texto: 559px de ancho × 660px de alto
  - Suma exacta de los 5 tamaños = 700px (calibrado empíricamente)
  - Rango por línea: 100-170px
  - Fuente: Anton condensed (CHAR_RATIO = 0.44)

Uso:
  python formatear_portadas.py                    # procesa todas las filas pendientes
  python formatear_portadas.py --fecha 2026-04-01 # solo esa fecha
  python formatear_portadas.py --dry-run          # muestra resultado sin guardar

Ubicación: C:\\docker\\projects\\canal-reli\\app\\utils\\formatear_portadas.py
"""

import os
import sys
import json
import logging
import argparse
import time
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# ============================================
# RUTAS
# ============================================
PROJECT_ROOT  = Path(__file__).parent.parent.parent
CONFIG_DIR    = PROJECT_ROOT / "config"
DATA_DIR      = PROJECT_ROOT / "data"
CALENDARIO_CSV = DATA_DIR / "input" / "calendario.csv"
LOGS_DIR      = DATA_DIR / "logs" / "portadas"

load_dotenv(PROJECT_ROOT / ".env")

import anthropic

# ============================================
# CONSTRAINTS DEL LAYOUT (calibrados con Anton + generar_portada.py)
# ============================================
# Canvas: 1280x720px | Borde: 14px | BORDER_TEXT_PAD: 16px
# Zona de texto: 559px ancho x 660px alto
#
# Suma maxima calibrada empiricamente: la combinacion mas grande
# que cabe sin salirse es [160, 150, 130, 150, 110] -> suma = 700px.
# Cualquier combinacion cuya suma <= 700 y que respete el constraint
# de ancho por linea es valida.

TEXT_ZONE_W  = 559    # px de ancho disponible para el texto
TARGET_SUM   = 700    # suma objetivo calibrada empiricamente
MAX_SIZE_CAP = 170    # tamano maximo absoluto por linea
MIN_SIZE     = 100    # tamano minimo absoluto por linea

# Rangos semanticos de tamano
SIZE_SMALL  = (100, 120)   # frases de enlace / poco relevantes / largas -> siempre blanco
SIZE_MEDIUM = (130, 140)   # palabras de peso medio -> amarillo o blanco
SIZE_LARGE  = (150, 170)   # palabras clave / impacto maximo -> amarillo o rojo

CHAR_RATIO   = 0.44   # ratio ancho_char / font_size para Anton condensed (calibrado)
SPACE_RATIO  = 0.26   # ratio ancho espacio / font_size (= 0.44 * 0.6)

# Colores disponibles (deben coincidir con COLOR_MAP en generar_portada.py)
COLORES_VALIDOS = ["amarillo", "blanco", "rojo"]


# ============================================
# LOGGING
# ============================================

def setup_logging():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    from datetime import datetime
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOGS_DIR / f"formatear_{ts}.log", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = None


# ============================================
# CÁLCULO DE TAMAÑO MÁXIMO POR LÍNEA (constraint de ancho)
# ============================================

def max_size_for_line(texto: str) -> int:
    """
    Calcula el tamaño máximo de fuente para que una línea quepa en 559px.
    Usa Anton condensed: chars × 0.58 + espacios × 0.35, todo × font_size.
    El resultado se limita al rango [MIN_SIZE, MAX_SIZE_CAP].
    """
    n = len(texto.replace(" ", ""))
    s = texto.count(" ")
    ratio = n * CHAR_RATIO + s * SPACE_RATIO
    if ratio == 0:
        return MAX_SIZE_CAP
    return max(MIN_SIZE, min(int(TEXT_ZONE_W / ratio), MAX_SIZE_CAP))


def clasificar_linea(texto: str, max_px: int) -> str:
    """
    Clasifica una línea según su longitud y su máximo permitido por ancho.
    Devuelve: 'small' | 'medium' | 'large'
    Esta clasificación es una PISTA para Claude, no una regla rígida.
    """
    if max_px <= SIZE_SMALL[1]:
        return "small"
    elif max_px <= SIZE_MEDIUM[1]:
        return "medium"
    else:
        return "large"


# ============================================
# LLAMADA A CLAUDE — toma de decisiones creativas
# ============================================

def formatear_lineas(lineas: list[str], client: anthropic.Anthropic) -> dict:
    """
    Llama a Claude para que asigne tamaño y color a cada línea.

    Proceso:
      1. Calcula el máximo de ancho por línea (constraint físico).
      2. Ajusta proporcionalmente si la suma de máximos supera TARGET_SUM.
      3. Envía a Claude las líneas, sus límites y las reglas de diseño.
      4. Claude devuelve JSON con tamanos y colores.
      5. _validar_resultado() corrige cualquier violación de constraints duros.

    Claude tiene libertad creativa dentro de los rangos: decide qué palabras
    son más relevantes, qué color impacta más en cada contexto, y cuánto
    contraste de tamaño aplicar. Los constraints duros (suma, rango, colores)
    se aplican después como capa de corrección, no dentro del prompt.
    """
    maximos = [max_size_for_line(l) for l in lineas]
    clases  = [clasificar_linea(l, m) for l, m in zip(lineas, maximos)]

    # Si la suma de máximos supera el objetivo, escalar proporcionalmente
    suma_max = sum(maximos)
    if suma_max > TARGET_SUM:
        factor  = TARGET_SUM / suma_max
        maximos = [max(MIN_SIZE, int(m * factor)) for m in maximos]

    # Detectar si la línea 1 es "SALMO X"
    es_salmo = lineas[0].strip().upper().startswith("SALMO")

    lineas_str = ""
    for i, (l, m, c) in enumerate(zip(lineas, maximos, clases)):
        nota = ""
        if i == 0 and es_salmo:
            nota = "  ← SALMO: fijar en 160px"
        elif c == "small":
            nota = "  ← frase de enlace o larga: rango 100-120, color blanco"
        elif c == "large":
            nota = "  ← palabra clave corta: rango 150-170, color amarillo o rojo"
        else:
            nota = "  ← peso medio: rango 130-140, amarillo o blanco según relevancia"
        lineas_str += f'  Linea {i+1}: "{l}" (max {m}px, clase={c}){nota}\n'

    salmo_nota = ""
    if es_salmo:
        salmo_nota = """
REGLA ESPECIAL — SALMO X (linea 1):
  - Tamano fijo: 160px.
  - Color: amarillo en el 80% de los casos, rojo en el 20%.
  - Si la linea 1 es amarillo → la siguiente linea relevante grande DEBE ser roja.
  - Si la linea 1 es rojo     → la siguiente linea relevante grande DEBE ser amarilla.
  - Esta regla garantiza que siempre haya contraste entre las dos lineas de mayor impacto.
"""

    prompt = f"""Eres un diseñador gráfico experto en portadas de YouTube de contenido cristiano.
Tu tarea: asignar tamaño de fuente y color a cada una de las 5 líneas de texto de una portada.

ZONA DE TEXTO: 559px ancho × 660px alto | Fuente: Anton (condensada, mayúsculas)
SUMA TOTAL DE LOS 5 TAMAÑOS: debe ser exactamente {TARGET_SUM}px.
RANGO PERMITIDO POR LÍNEA: entre {MIN_SIZE}px y {MAX_SIZE_CAP}px.
MÍNIMO 3 TAMAÑOS DISTINTOS en el conjunto.
MÍNIMO 1 LÍNEA de cada color (amarillo, blanco, rojo) — toda portada usa los tres.

LÍNEAS:
{lineas_str}
{salmo_nota}
REGLAS DE TAMAÑO:
  - Rango 100-120 (pequeño): frases de enlace, preposiciones, frases largas.
                              SIEMPRE color blanco. Sin excepciones.
  - Rango 130-140 (medio):   palabras de peso moderado. Color amarillo o blanco
                              según relevancia. Nunca rojo salvo que sea la única
                              línea de alto impacto y no haya otra candidata.
  - Rango 150-170 (grande):  palabras clave cortas, verbos de acción, conceptos
                              de impacto. Color amarillo o rojo. Nunca blanco.

REGLAS DE COLOR:
  - "amarillo": palabras de énfasis principal (SALMO, verbo clave, concepto central).
  - "rojo":     impacto máximo. Solo UNA línea por portada. La de mayor carga emocional.
  - "blanco":   soporte, enlace, contexto. Contrasta visualmente con amarillo y rojo.
  - Nunca dos líneas consecutivas del mismo color si hay alternativa.

CRITERIO DE RELEVANCIA (guía para tus decisiones):
  Alta relevancia  → tamaño grande (150-170) + amarillo o rojo.
  Media relevancia → tamaño medio  (130-140) + amarillo o blanco.
  Baja relevancia  → tamaño pequeño (100-120) + blanco.
  Las líneas más cortas tienden a ser las más relevantes visualmente
  (más espacio para un tamaño mayor).

EJEMPLOS CORRECTOS:
  ["SALMO 91", "DESTRUYE", "TUS ENEMIGOS", "OCULTOS", "EN 24 HORAS"]
  tamanos: [160, 150, 100, 160, 130]  suma=700
  colores: ["amarillo", "rojo", "blanco", "amarillo", "blanco"]

  ["EL SALMO", "PROHIBIDO", "QUE TE DESTRUYE", "POR DENTRO", "AL INSTANTE"]
  tamanos: [150, 160, 110, 120, 160]  suma=700
  colores: ["blanco", "rojo", "blanco", "blanco", "amarillo"]

  ["SALMO 35", "QUEMA", "LOS HECHIZOS", "CONTRA TU HOGAR", "AL INSTANTE"]
  tamanos: [160, 170, 130, 110, 130]  suma=700
  colores: ["amarillo", "rojo", "blanco", "blanco", "amarillo"]

Responde ÚNICAMENTE con JSON válido, sin markdown ni texto extra:
{{"tamanos": [t1, t2, t3, t4, t5], "colores": ["c1", "c2", "c3", "c4", "c5"]}}"""

    try:
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        text = msg.content[0].text.strip()

        # Extraer el primer objeto JSON del texto, ignorando cualquier
        # contenido extra que Claude añada antes o después.
        # Resuelve el error "Extra data: line N column 1".
        import re as _re
        match = _re.search(r'\{.*?"tamanos".*?\}', text, _re.DOTALL)
        if not match:
            raise ValueError(f"No se encontro JSON en la respuesta: {text[:120]}")
        data    = json.loads(match.group())
        tamanos = [int(x) for x in data["tamanos"]]
        colores = [str(x).lower().strip() for x in data["colores"]]
        return _validar_resultado(tamanos, colores, maximos, lineas)

    except Exception as e:
        logger.warning(f"    ⚠️  Error en Claude: {e} — usando fallback")
        return _fallback(lineas, maximos)


def _validar_resultado(
    tamanos: list, colores: list, maximos: list, lineas: list
) -> dict:
    """
    Aplica los constraints duros DESPUÉS de la respuesta de Claude.
    Claude toma las decisiones creativas; aquí solo corregimos violaciones.
    """
    tamanos = tamanos[:5]
    colores = colores[:5]

    # 1. Rango por línea: [MIN_SIZE, min(max_por_ancho, MAX_SIZE_CAP)]
    for i in range(5):
        cap = min(maximos[i], MAX_SIZE_CAP)
        tamanos[i] = max(MIN_SIZE, min(tamanos[i], cap))

    # 2. Suma exacta = TARGET_SUM
    #    Ajustar iterativamente: subir/bajar la línea más grande/pequeña
    intentos = 0
    while sum(tamanos) != TARGET_SUM and intentos < 50:
        diff = TARGET_SUM - sum(tamanos)
        if diff > 0:
            # Necesitamos subir → aumentar la línea más grande con margen
            candidatos = [i for i in range(5)
                          if tamanos[i] < min(maximos[i], MAX_SIZE_CAP)]
            if not candidatos:
                break
            idx = max(candidatos, key=lambda i: tamanos[i])
            tamanos[idx] = min(tamanos[idx] + min(diff, 10),
                               min(maximos[idx], MAX_SIZE_CAP))
        else:
            # Necesitamos bajar → reducir la línea más grande
            idx = tamanos.index(max(tamanos))
            tamanos[idx] = max(tamanos[idx] + max(diff, -10), MIN_SIZE)
        intentos += 1

    # 3. Mínimo 3 tamaños distintos
    if len(set(tamanos)) < 3:
        # Forzar variación en las posiciones medias sin romper la suma
        for i in [2, 3]:
            if tamanos[i] > MIN_SIZE + 10:
                tamanos[i] -= 10
                tamanos[0]  = min(tamanos[0] + 10, min(maximos[0], MAX_SIZE_CAP))
            if len(set(tamanos)) >= 3:
                break

    # 4. Colores válidos
    colores = [c if c in COLORES_VALIDOS else "blanco" for c in colores]

    # 5. Solo UN rojo
    rojos = [i for i, c in enumerate(colores) if c == "rojo"]
    if len(rojos) > 1:
        # Conservar el rojo en la línea de mayor tamaño
        idx_keep = max(rojos, key=lambda i: tamanos[i])
        colores = ["rojo" if i == idx_keep else
                   ("amarillo" if c == "rojo" else c)
                   for i, c in enumerate(colores)]

    # 6. Los tres colores deben aparecer
    for color_req in ["amarillo", "blanco", "rojo"]:
        if color_req not in colores:
            # Asignar al candidato más apropiado
            if color_req == "rojo":
                # La línea más grande sin rojo ni amarillo ya asignado
                candidatos = [i for i, c in enumerate(colores) if c != "rojo"]
                idx = max(candidatos, key=lambda i: tamanos[i])
                colores[idx] = "rojo"
                # Corregir doble rojo si lo hay
                rojos = [i for i, c in enumerate(colores) if c == "rojo"]
                if len(rojos) > 1:
                    idx_keep = max(rojos, key=lambda i: tamanos[i])
                    colores = ["rojo" if i == idx_keep else
                               ("amarillo" if c == "rojo" else c)
                               for i, c in enumerate(colores)]
            elif color_req == "amarillo":
                candidatos = [i for i, c in enumerate(colores)
                              if c == "blanco" and tamanos[i] >= SIZE_MEDIUM[0]]
                if candidatos:
                    colores[candidatos[0]] = "amarillo"
            elif color_req == "blanco":
                candidatos = [i for i, c in enumerate(colores)
                              if c == "amarillo" and tamanos[i] <= SIZE_MEDIUM[1]]
                if candidatos:
                    colores[candidatos[0]] = "blanco"

    # 7. Constraint de color por rango: rango pequeño → siempre blanco
    for i, (t, c) in enumerate(zip(tamanos, colores)):
        if t <= SIZE_SMALL[1] and c != "blanco":
            # Reasignar su color a la línea grande más cercana
            grandes = [j for j in range(5) if tamanos[j] >= SIZE_LARGE[0] and j != i]
            if grandes and c == "rojo":
                colores[grandes[0]] = "rojo"
            colores[i] = "blanco"

    # 8. Constraint SALMO: si la primera línea es SALMO X → tamaño 160
    if lineas[0].strip().upper().startswith("SALMO"):
        diff_salmo = 160 - tamanos[0]
        tamanos[0] = 160
        # Redistribuir la diferencia en la línea más grande restante
        if diff_salmo != 0:
            resto = [i for i in range(1, 5)]
            idx_adj = max(resto, key=lambda i: tamanos[i]) if diff_salmo < 0                       else min(resto, key=lambda i: tamanos[i])
            tamanos[idx_adj] = max(MIN_SIZE,
                                   min(tamanos[idx_adj] - diff_salmo, MAX_SIZE_CAP))

    return {"tamanos": tamanos, "colores": colores}


def _fallback(lineas: list, maximos: list) -> dict:
    """
    Asignación determinista cuando Claude falla completamente.
    Detecta SALMO, asigna jerarquía por longitud, distribuye colores.
    """
    es_salmo = lineas[0].strip().upper().startswith("SALMO")
    longitudes = [len(l) for l in lineas]

    # Asignar tamaños por longitud inversa (más corta → más grande)
    orden = sorted(range(5), key=lambda i: longitudes[i])
    escala = [170, 160, 140, 120, 100]
    tamanos = [0] * 5
    for rank, idx in enumerate(orden):
        tamanos[idx] = escala[rank]

    if es_salmo:
        tamanos[0] = 160

    # Ajustar suma a TARGET_SUM
    while sum(tamanos) > TARGET_SUM:
        idx_max = tamanos.index(max(tamanos))
        tamanos[idx_max] = max(MIN_SIZE, tamanos[idx_max] - 10)
    while sum(tamanos) < TARGET_SUM:
        idx_min = tamanos.index(min(tamanos))
        tamanos[idx_min] = min(tamanos[idx_min] + 10, MAX_SIZE_CAP)

    # Colores: amarillo en línea más grande, rojo en segunda más grande, blanco el resto
    orden_tam = sorted(range(5), key=lambda i: tamanos[i], reverse=True)
    colores = ["blanco"] * 5
    colores[orden_tam[0]] = "amarillo" if not es_salmo else "amarillo"
    colores[orden_tam[1]] = "rojo"
    if es_salmo:
        colores[0] = "amarillo"

    return {"tamanos": tamanos, "colores": colores}


# ============================================
# PROCESAMIENTO DEL CSV
# ============================================

def _es_vacio(valor) -> bool:
    """Devuelve True si la celda está vacía, NaN o solo espacios."""
    import pandas as pd
    return pd.isna(valor) or str(valor).strip() == ""


def procesar_calendario(fecha_filtro: str | None, dry_run: bool, forzar: bool):
    """
    Lee calendario.csv, detecta filas con CONTENIDO sin TAMAÑOS/COLORES
    y rellena ambas columnas llamando a Claude.

    Parámetros:
        fecha_filtro : procesar solo esa fecha (YYYY-MM-DD). None = todas.
        dry_run      : calcular maximos por ancho y mostrar resultado SIN
                       llamar a Claude ni escribir el CSV.
        forzar       : reprocesar también filas que ya tienen TAMAÑOS/COLORES.
    """
    if not CALENDARIO_CSV.exists():
        logger.error(f"No se encuentra: {CALENDARIO_CSV}")
        sys.exit(1)

    df = pd.read_csv(CALENDARIO_CSV, encoding="utf-8", dtype=str)
    logger.info(f"Calendario cargado: {len(df)} filas | columnas: {list(df.columns)}")

    # Garantizar que las columnas de salida existen
    for col in ["TAMAÑOS", "COLORES"]:
        if col not in df.columns:
            df[col] = ""

    # --- Selección de filas a procesar ---
    def necesita_formato(row) -> bool:
        if _es_vacio(row.get("CONTENIDO", "")):
            return False                          # sin contenido: saltar siempre
        if forzar:
            return True                           # --forzar: reprocesar todo
        return _es_vacio(row.get("TAMAÑOS", "")) or _es_vacio(row.get("COLORES", ""))

    if fecha_filtro:
        mask = (df["FECHA"] == fecha_filtro) & df.apply(necesita_formato, axis=1)
    else:
        mask = df.apply(necesita_formato, axis=1)

    filas = df[mask]
    logger.info(f"Filas a formatear: {len(filas)}")

    if len(filas) == 0:
        logger.info("No hay filas pendientes")
        return

    # --- Cliente Anthropic (solo si no es dry-run) ---
    client = None
    if not dry_run:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            logger.error("ANTHROPIC_API_KEY no encontrada en .env")
            sys.exit(1)
        client = anthropic.Anthropic(api_key=api_key)

    procesadas = errores = 0

    for idx, row in filas.iterrows():
        fecha  = str(row.get("FECHA", "?"))
        titulo = str(row.get("TÍTULO", ""))[:55]

        try:
            # Parsear CONTENIDO: acepta JSON array o separado por |
            raw = str(row["CONTENIDO"]).strip()
            if raw.startswith("["):
                lineas = json.loads(raw)
            else:
                lineas = [l.strip() for l in raw.split("|") if l.strip()]

            if len(lineas) != 5:
                logger.warning(f"[{fecha}] CONTENIDO no tiene 5 lineas ({len(lineas)}) — saltando")
                continue

            # Normalizar a mayúsculas (el layout usa .upper() pero mejor almacenar limpio)
            lineas = [l.upper().strip() for l in lineas]

            if dry_run:
                maximos = [max_size_for_line(l) for l in lineas]
                clases  = [clasificar_linea(l, m) for l, m in zip(lineas, maximos)]
                logger.info(f"[{fecha}] {titulo}")
                for l, m, c in zip(lineas, maximos, clases):
                    logger.info(f"  '{l}' → max {m}px [{c}]")
                logger.info(f"  Suma maximos: {sum(maximos)}px | TARGET: {TARGET_SUM}px")
                procesadas += 1
                continue

            logger.info(f"[{fecha}] {titulo}...")
            resultado = formatear_lineas(lineas, client)
            tamanos   = resultado["tamanos"]
            colores   = resultado["colores"]

            logger.info(f"  tamanos={tamanos} suma={sum(tamanos)} | colores={colores}")

            df.at[idx, "TAMAÑOS"] = json.dumps(tamanos, ensure_ascii=False)
            df.at[idx, "COLORES"] = json.dumps(colores, ensure_ascii=False)
            procesadas += 1

            if procesadas < len(filas):
                time.sleep(0.4)   # pausa mínima para evitar rate-limit

        except Exception as e:
            logger.error(f"[{fecha}] Error: {e}")
            errores += 1

    # Guardar CSV solo si hubo cambios reales
    if not dry_run and procesadas > 0:
        df.to_csv(CALENDARIO_CSV, index=False, encoding="utf-8")
        logger.info(f"calendario.csv guardado — {procesadas} filas actualizadas, {errores} errores")
    elif dry_run:
        logger.info(f"[DRY RUN] {procesadas} filas analizadas, nada guardado")
    else:
        logger.info(f"Nada guardado — procesadas={procesadas}, errores={errores}")


# ============================================
# MAIN
# ============================================

def main():
    global logger
    logger = setup_logging()

    from datetime import datetime
    logger.info("=" * 56)
    logger.info("FORMATEADOR DE PORTADAS — Canal Reli")
    logger.info("=" * 56)
    logger.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    parser = argparse.ArgumentParser(
        description="Asigna TAMAÑOS y COLORES a portadas del calendario"
    )
    parser.add_argument(
        "--fecha", type=str, default=None,
        help="Procesar solo esta fecha (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Analizar sin llamar a Claude ni guardar"
    )
    parser.add_argument(
        "--forzar", action="store_true",
        help="Reprocesar filas aunque ya tengan TAMAÑOS y COLORES"
    )
    args = parser.parse_args()

    procesar_calendario(
        fecha_filtro=args.fecha,
        dry_run=args.dry_run,
        forzar=args.forzar,
    )
    logger.info("Siguiente paso: generar_portada.py --fecha YYYY-MM-DD")


if __name__ == "__main__":
    main()