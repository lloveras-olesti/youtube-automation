#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================
PIPELINE DE GENERACIÓN DE ORACIONES DEVOCIONALES
================================================
Versión adaptada para Docker con calendario CSV

Automatiza la generación completa de una oración devocional (~4500 palabras)
utilizando Claude AI (Haiku 4.5 para el cuerpo, Sonnet 4.5 para la intro).

FLUJO:
  1. Lee datos del CSV (calendario.csv)
  2. Genera cuerpo principal con Haiku 4.5 (~4500 palabras)
  3. Genera introducción con Sonnet 4.5 (~300 palabras)
  4. Obtiene texto del Salmo con Haiku 4.5 (RVR 1909)
  5. Ensamble: Intro + Salmo reemplazan los 3 primeros párrafos del cuerpo
  6. Guarda resultado en Oracion.txt

USO:
  python generar_txt.py              → Procesa la fila cuya fecha = hoy
  python generar_txt.py --fila 1     → Procesa la 1ª fila de datos
  python generar_txt.py --fila 2     → Procesa la 2ª fila de datos

REQUISITOS:
  1. Python 3.8+
  2. pip install anthropic pandas
  3. API Key de Anthropic en archivo .env
"""

import os
import re
import sys
import argparse
import logging

import anthropic
import pandas as pd
from app.config import settings, PROMPT_STYLES, PROMPT_INTROS, PROMPT_MASTER_FILE



# ╔══════════════════════════════════════════════════════════════╗
# ║  RUTAS Y MAPEOS (usando config.py)                          ║
# ╚══════════════════════════════════════════════════════════════╝

CSV_PATH = settings.calendario_path
OUTPUT_PATH = settings.temp_path / "guion.txt"
INTRO_TEMP_PATH = settings.temp_path / "intro_temp.txt"

# Modelos
HAIKU_MODEL = settings.claude_haiku_model
SONNET_MODEL = settings.claude_sonnet_model

# ╚══════════════════════════════════════════════════════════════╝

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            str(settings.logs_path / "guion_generator.log"),
            encoding="utf-8"
        )
    ]
)
logger = logging.getLogger(__name__)


# ╔══════════════════════════════════════════════════════════════╗
# ║  UTILIDADES                                                  ║
# ╚══════════════════════════════════════════════════════════════╝

def read_file(path: str) -> str:
    """Lee un archivo .txt y devuelve su contenido."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Archivo no encontrado: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def write_file(path: str, content: str) -> None:
    """Escribe contenido en un archivo .txt."""
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    logger.info("  -> Guardado: %s", path)


# ╔══════════════════════════════════════════════════════════════╗
# ║  PASO 1 — LECTURA DEL CSV                                    ║
# ╚══════════════════════════════════════════════════════════════╝

def get_row_data(csv_path: str, row_index: int = 1) -> dict:
    """
    Lee calendario.csv y devuelve los datos de una fila.

    row_index=1 (por defecto) → primera fila de datos
    row_index=2               → segunda fila, etc.

    Devuelve dict con: fecha, titulo, salmo, texto_ref, intro_ref
    """
    df = pd.read_csv(csv_path, encoding='utf-8')

    # row_index 1 = primera fila (índice 0 del DataFrame)
    df_index = row_index - 1
    
    if df_index >= len(df):
        raise ValueError(
            f"Fila {row_index} no existe. "
            f"Filas disponibles: 1 a {len(df)}"
        )

    row = df.iloc[df_index]
    
    fecha = row['FECHA']
    titulo = row['TÍTULO']
    salmo = row['SALMO']
    texto_ref = row['TEXTO']
    intro_ref = row['INTRO']

    # Normalizar valores
    texto_ref = str(texto_ref).strip().lower()   # "r1", "r2", "r3"
    intro_ref = str(intro_ref).strip().lower()   # "i1", "i2"
    salmo_str = str(int(float(salmo)))           # Convierte 29.0 → "29"

    # Validar que las referencias existan en los mapeos
    if texto_ref not in PROMPT_STYLES:
        raise ValueError(
            f"Columna TEXTO contiene '{texto_ref}'. "
            f"Valores válidos: {list(PROMPT_STYLES.keys())}"
        )
    if intro_ref not in PROMPT_INTROS:
        raise ValueError(
            f"Columna INTRO contiene '{intro_ref}'. "
            f"Valores válidos: {list(PROMPT_INTROS.keys())}"
        )

    return {
        "fecha":     fecha,
        "titulo":    titulo,
        "salmo":     salmo_str,
        "texto_ref": texto_ref,
        "intro_ref": intro_ref,
    }


# ╔══════════════════════════════════════════════════════════════╗
# ║  PASO 2 — CUERPO PRINCIPAL (HAIKU 4.5, ~4500 palabras)      ║
# ╚══════════════════════════════════════════════════════════════╝

def generate_main_body(titulo: str, salmo: str, texto_ref: str) -> str:
    """
    Genera el cuerpo de la oración (~4500 palabras) con Haiku 4.5.

    Proceso:
      a) Lee ORACIÓN_B.txt (prompt maestro)
      b) Lee rX.txt (texto de referencia estilística, ~3000 palabras)
      c) Sustituye las 3 variables: [TÍTULO], número de Salmo, [TEXTO]
      d) Añade instrucción de ejecución (refuerzo)
      e) Envía a Claude Haiku 4.5
    """
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    # a) Leer archivos
    prompt_maestro = settings.read_prompt("especificos", PROMPT_MASTER_FILE)
    texto_referencia = settings.read_prompt("estilos", PROMPT_STYLES[texto_ref])

    # Normalizar saltos de línea
    prompt_maestro = prompt_maestro.replace("\r\n", "\n")

    # c) Sustituir variables ─────────────────────────────────────────

    # Variable 1: [TÍTULO]
    prompt_maestro = prompt_maestro.replace("[TÍTULO]", titulo)

    # Variable 2: Número de Salmo
    prompt_maestro = re.sub(
        r"(\*\*Salmo especificado:\*\*\s*`?)\[[^\]]*\](`?)",
        rf"\g<1>[{salmo}]\2",
        prompt_maestro
    )

    # Variable 3: [TEXTO] → contenido completo del archivo de referencia
    prompt_maestro = prompt_maestro.replace("[TEXTO]", texto_referencia)

    # d) Instrucción de ejecución ─────────────────────────────────────
    instruccion = (
        "Esta referencia es un prompt. Ejecutalo para generar un texto de 4500 palabras. "
        "Cita varias veces el salmo especificado, relacionándolo con la temática de la oración. "
        "Habla sobre escenas de la Biblia que sean coherentes con la temática de la oración. "
        "Evita repetir temáticas y sé variado en las introducciones de párrafo. "
        "No hagas frases introductorias a secciones, quiero un texto lineal. "
        'Limitar el inicio de párrafos en primera persona del presente con la estructura '
        'verbo + complemento (ej.: "Desato…", "Quebro…") a máx. 15% del total de párrafos. '
        "No usar ese mismo tipo de inicio en párrafos consecutivos.\n\n"
        f"**TEMÁTICA DE LA ORACIÓN:** {titulo}\n"
        f"**SALMO:** {salmo}\n\n"
        "Genera el texto completo ahora."
    )

    prompt_completo = prompt_maestro + "\n\n" + instruccion

    logger.info("  Enviando a Haiku 4.5...")
    response = client.messages.create(
        model=HAIKU_MODEL,
        max_tokens=16000,
        messages=[{"role": "user", "content": prompt_completo}]
    )

    resultado = response.content[0].text
    logger.info("  Cuerpo generado: %s palabras", len(resultado.split()))
    return resultado


# ╔══════════════════════════════════════════════════════════════╗
# ║  PASO 3 — INTRODUCCIÓN (SONNET 4.5, ~300 palabras)         ║
# ╚══════════════════════════════════════════════════════════════╝

def generate_introduction(titulo: str, salmo: str, intro_ref: str) -> str:
    """
    Genera la introducción (~300 palabras) con Sonnet 4.5.

    Lee iX.txt, sustituye [TÍTULO] y [SALMO] en todas las ocurrencias,
    y envía a Sonnet.
    """
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    prompt_intro = settings.read_prompt("intros", PROMPT_INTROS[intro_ref])
    prompt_intro = prompt_intro.replace("\r\n", "\n")

    # Sustituir variables
    prompt_intro = prompt_intro.replace("[TÍTULO]", titulo)
    prompt_intro = prompt_intro.replace("[SALMO]", salmo)

    logger.info("  Enviando a Sonnet (plantilla: %s)...", intro_ref)

    response = client.messages.create(
        model=SONNET_MODEL,
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt_intro}]
    )

    resultado = response.content[0].text
    logger.info("  Introducción generada: %s palabras", len(resultado.split()))
    return resultado


# ╔══════════════════════════════════════════════════════════════╗
# ║  PASO 4 — TEXTO DEL SALMO (HAIKU 4.5, versión RVR 1909)    ║
# ╚══════════════════════════════════════════════════════════════╝

def generate_psalm(salmo: str) -> str:
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    system_prompt = (
        "Eres un asistente especializado en textos bíblicos históricos. "
        "Proporcionas textos de la Biblia Reina Valera Revisada 1909 (dominio público) "
        "para estudio y referencia académica. Cuando se te solicite un salmo, "
        "devuelves únicamente el texto bíblico sin añadir comentarios, explicaciones "
        "o material adicional."
    )

    prompt = (
        f"Salmo {salmo} completo en RVR 1909.\n\n"
        f"Formato requerido:\n"
        f"- Texto corrido sin números de versículo\n"
        f"- Omitir encabezados como 'Salmo de David' o similares\n"
        f"- Solo el contenido del salmo en español\n"
        f"- Sin introducción ni explicaciones"
    )

    logger.info("  Solicitando Salmo %s a Haiku...", salmo)

    response = client.messages.create(
        model=HAIKU_MODEL,
        max_tokens=2048,
        system=system_prompt,
        messages=[{"role": "user", "content": prompt}]
    )

    resultado = response.content[0].text.strip()
    logger.info("  Salmo %s obtenido: %s palabras",
                salmo, len(resultado.split()))
    return resultado


# ╔══════════════════════════════════════════════════════════════╗
# ║  PASO 5 — ENSAMBLAJE FINAL                                   ║
# ╚══════════════════════════════════════════════════════════════╝

def replace_first_three_paragraphs(cuerpo: str, reemplazo: str) -> str:
    """
    Reemplaza los 3 primeros párrafos del cuerpo con el texto de reemplazo
    (introducción + salmo).

    Los párrafos se identifican por líneas en blanco (\\n\\n), que es el
    separador estándar que produce Claude en sus respuestas.
    """
    # Normalizar y dividir en párrafos
    cuerpo_clean = cuerpo.replace("\r\n", "\n").strip()
    parrafos = re.split(r"\n\n+", cuerpo_clean)

    logger.info("  Párrafos detectados en el cuerpo: %s", len(parrafos))

    if len(parrafos) < 4:
        logger.warning(
            "  AVISO: Solo %s párrafos detectados (necesitan >= 4). "
            "El reemplazo se prepone al texto completo.",
            len(parrafos)
        )
        return reemplazo.strip() + "\n\n" + cuerpo_clean

    # Conservar desde el párrafo 4 en adelante (índice 3)
    resto = "\n\n".join(parrafos[3:])

    # Resultado: reemplazo (intro + salmo) + resto del cuerpo
    texto_final = reemplazo.strip() + "\n\n" + resto

    logger.info(
        "  Primeros 3 párrafos reemplazados. Párrafos restantes: %s", len(parrafos) - 3)
    return texto_final


# ╔══════════════════════════════════════════════════════════════╗
# ║  PIPELINE PRINCIPAL                                          ║
# ╚══════════════════════════════════════════════════════════════╝

def run_pipeline(row_index: int = 1):
    """
    Ejecuta la pipeline completa.
    """
    logger.info("\n" + "=" * 60)
    logger.info("  PIPELINE DE GENERACIÓN DE ORACIÓN — INICIO")
    logger.info("=" * 60)

    # ─── PASO 1: Datos del CSV ──────────────────────────────
    logger.info("\n[1/5] Leyendo datos del CSV...")
    datos = get_row_data(CSV_PATH, row_index=row_index)

    titulo = datos["titulo"]
    salmo = datos["salmo"]
    texto_ref = datos["texto_ref"]
    intro_ref = datos["intro_ref"]

    logger.info("  Título:    %s", titulo)
    logger.info("  Salmo:     %s", salmo)
    logger.info("  Ref texto: %s  ->  %s", texto_ref, settings.read_prompt("estilos", PROMPT_STYLES[texto_ref]))
    logger.info("  Ref intro: %s  ->  %s", intro_ref, settings.read_prompt("intros", PROMPT_INTROS[intro_ref]))

    # ─── PASO 2: Cuerpo principal con Haiku ───────────────────
    logger.info("\n[2/5] Generando cuerpo principal con Haiku 4.5...")
    cuerpo = generate_main_body(titulo, salmo, texto_ref)

    # ─── PASO 3: Introducción con Sonnet ──────────────────────
    logger.info("\n[3/5] Generando introducción con Sonnet 4.5...")
    introduccion = generate_introduction(titulo, salmo, intro_ref)

    # Guardar intro en archivo temporal
    write_file(INTRO_TEMP_PATH, introduccion)

    # ─── PASO 4: Texto del Salmo con Haiku ────────────────────
    logger.info("\n[4/5] Obteniendo texto del Salmo %s con Haiku...", salmo)
    salmo_texto = generate_psalm(salmo)

    # ─── PASO 5: Ensamblaje ───────────────────────────────────
    logger.info("\n[5/5] Ensamblando texto final...")

    # 5a) Añadir Salmo al final de la introducción
    intro_con_salmo = introduccion.rstrip() + "\n\n" + salmo_texto

    # 5b) Reemplazar los 3 primeros párrafos del cuerpo con intro + salmo
    texto_final = replace_first_three_paragraphs(cuerpo, intro_con_salmo)

    # 5c) Guardar archivo final de salida
    write_file(OUTPUT_PATH, texto_final)

    # ─── Resumen ──────────────────────────────────────────────
    logger.info("\n" + "=" * 60)
    logger.info("  PIPELINE COMPLETADA EXITOSAMENTE")
    logger.info("=" * 60)
    logger.info("  Archivo final:      %s", OUTPUT_PATH)
    logger.info("  Intro temporal:     %s", INTRO_TEMP_PATH)
    logger.info("  Palabras totales:   %s", len(texto_final.split()))
    logger.info("    Introduccion:     %s", len(introduccion.split()))
    logger.info("    Salmo:            %s", len(salmo_texto.split()))
    logger.info("=" * 60 + "\n")


# ╔══════════════════════════════════════════════════════════════╗
# ║  ENTRY POINT                                                 ║
# ╚══════════════════════════════════════════════════════════════╝

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generador automático de Oraciones Devocionales con Claude AI"
    )
    parser.add_argument(
        "--fila",
        type=int,
        default=1,
        help="Fila a procesar (1 = primera de datos)"
    )
    args = parser.parse_args()

    try:
        run_pipeline(row_index=args.fila)

    except FileNotFoundError as e:
        logger.error("ERROR: %s", e)
        sys.exit(1)

    except ValueError as e:
        logger.error("ERROR: %s", e)
        sys.exit(1)

    except anthropic.AuthenticationError:
        logger.error(
            "ERROR: API Key inválida.\n"
            "  Verifica que la clave en el archivo .env sea correcta.\n"
            "  Obtén una nueva en: https://console.anthropic.com/"
        )
        sys.exit(1)

    except anthropic.RateLimitError:
        logger.error(
            "ERROR: Rate limit alcanzado. Espera unos minutos e intenta de nuevo.")
        sys.exit(1)

    except anthropic.APIStatusError as e:
        logger.error("ERROR de API (%s): %s", e.status_code, e.message)
        if "model" in str(e.message).lower():
            logger.error(
                "  Si el error menciona el modelo, verifica los nombres en:\n"
                "  https://docs.claude.com/en/docs/about-claude/models/overview"
            )
        sys.exit(1)

    except Exception as e:
        logger.error("ERROR inesperado: %s", e, exc_info=True)
        sys.exit(1)
