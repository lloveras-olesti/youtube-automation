#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
====================================================
GENERADOR DE METADATOS SEO PARA YOUTUBE
====================================================
Versión con estructura libre — Claude Sonnet 4.6
"""

import os
import sys
import argparse
import logging

import anthropic
import pandas as pd
from app.config import settings

API_KEY = os.environ.get("ANTHROPIC_API_KEY")
if not API_KEY:
    print("ERROR: ANTHROPIC_API_KEY no está configurada en el archivo .env")
    sys.exit(1)

DATA_PATH = os.environ.get("DATA_PATH", "/app/data")
CSV_PATH = str(settings.calendario_path)
CANAL_URL = settings.canal_url
SONNET_MODEL = settings.claude_sonnet_model

# Etiquetas de canal: fijas en todos los videos.
# Definen el nicho y ayudan a YouTube a agrupar el contenido del canal.
# Longitud: ~120 caracteres. No modificar salvo cambio de línea editorial.
ETIQUETAS_CANAL = "oración poderosa, guerra espiritual, oración cristiana, liberación espiritual, oración católica, salmo poderoso, oración de liberación, protección divina"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            str(settings.logs_path / "seo_generator.log"),
            encoding="utf-8"
        )
    ]
)
logger = logging.getLogger(__name__)


def get_titulo_from_csv(csv_path: str, row_index: int = 1) -> tuple:
    df = pd.read_csv(csv_path, encoding='utf-8')
    df_index = row_index - 1
    if df_index >= len(df):
        raise ValueError(f"Fila {row_index} no existe. Filas disponibles: 1 a {len(df)}")
    titulo = df.iloc[df_index]['TÍTULO']
    if pd.isna(titulo) or not titulo:
        raise ValueError(f"La fila {row_index} no tiene título")
    return titulo, df_index


def generate_seo_metadata(titulo: str) -> dict:
    client = anthropic.Anthropic(api_key=API_KEY)

    chars_canal = len(ETIQUETAS_CANAL) + 2  # +2 por la coma y el espacio que se añadirán
    chars_disponibles_min = 400 - chars_canal
    chars_disponibles_max = 450 - chars_canal

    prompt = f"""Eres un experto en SEO para YouTube especializado en contenido cristiano de guerra espiritual, oración y liberación en español para audiencia latinoamericana.

**TÍTULO DEL VIDEO:** {titulo}

Genera DOS elementos de metadatos optimizados para este video:

---

1) **DESCRIPCIÓN:**

REGLAS DE CALIDAD (no negociables):
- Las primeras 2-3 líneas deben estar CARGADAS de keywords naturales derivadas del título. Son lo que aparece en los resultados de búsqueda: son las más importantes para el SEO.
- El texto debe ser poderoso, urgente y directo. Sin rodeos, sin frases genéricas, sin relleno.
- Habla a alguien que está atravesando la situación del video ahora mismo. Conecta con su dolor o necesidad real.
- Usa un lenguaje de autoridad espiritual — no ruegues, declara y actúa.
- La longitud, estructura y formato son completamente libres. Puedes usar párrafos, viñetas con emojis, preguntas retóricas, listas, bloques cortos, lo que mejor sirva al contenido específico de este título. Varía el enfoque en cada generación.

FOOTER OBLIGATORIO al final (copia exactamente):
👉 Suscríbete aquí: {CANAL_URL}

[AQUÍ 10 HASHTAGS ESPECÍFICOS DEL TEMA, separados por espacios]

---

2) **ETIQUETAS ESPECÍFICAS:**

El sistema añadirá automáticamente estas etiquetas de canal fijas al inicio:
"{ETIQUETAS_CANAL}"
(~{chars_canal} caracteres ya usados)

Tu tarea es generar ÚNICAMENTE las etiquetas específicas del video que se añadirán a continuación. Deben ocupar entre {chars_disponibles_min} y {chars_disponibles_max} caracteres (contando comas y espacios).

ESTRATEGIA EN DOS CAPAS:

CAPA 1 — Etiquetas de categoría (tema amplio del video, 3-4 etiquetas):
Identifica el ámbito principal: familia, finanzas, salud, enemigos, brujería, ansiedad, matrimonio, trabajo, etc. Genera 3-4 etiquetas que cubran este ámbito de forma amplia (ej. para un video sobre matrimonio: "oración por el matrimonio, restauración matrimonial, matrimonio en crisis").

CAPA 2 — Etiquetas long-tail específicas (resto de caracteres disponibles):
Keywords exactas de alta intención de búsqueda derivadas del título. Piensa como alguien que está buscando exactamente este video en YouTube: ¿qué frase escribiría? Incluye variaciones con y sin el número del salmo si aplica, con y sin "poderosa", versiones con el problema específico descrito en el título.

REGLAS:
- Frases de 2-4 palabras, máximo 30 caracteres por etiqueta (contando espacios). Las etiquetas más largas serán truncadas automáticamente.
- Sin duplicar conceptos ya cubiertos por las etiquetas de canal fijas
- Sin números solos, sin tildes incorrectas, sin mayúsculas innecesarias
- Devuelve SOLO las etiquetas específicas, sin incluir las etiquetas de canal

---

FORMATO DE RESPUESTA (usa exactamente esta estructura):

---DESCRIPCION---
[Descripción aquí con footer incluido]

---ETIQUETAS---
[Solo las etiquetas específicas aquí]
---FIN---

IMPORTANTE: Los 10 hashtags deben ser específicos del tema del video, no genéricos.
"""

    logger.info("  Enviando a Sonnet 4.6...")
    response = client.messages.create(
        model=SONNET_MODEL,
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}]
    )
    resultado = response.content[0].text

    try:
        desc_start = resultado.find("---DESCRIPCION---") + len("---DESCRIPCION---")
        desc_end = resultado.find("---ETIQUETAS---")
        descripcion = resultado[desc_start:desc_end].strip()

        etiq_start = resultado.find("---ETIQUETAS---") + len("---ETIQUETAS---")
        etiq_end = resultado.find("---FIN---")
        etiquetas_especificas = resultado[etiq_start:etiq_end].strip()

        # Ensamblar etiquetas finales: canal + específicas
        etiquetas_finales = f"{ETIQUETAS_CANAL}, {etiquetas_especificas}"

        chars_totales = len(etiquetas_finales)
        if chars_totales < 400 or chars_totales > 450:
            logger.warning(
                f"  AVISO: Etiquetas totales = {chars_totales} caracteres "
                f"(objetivo: 400-450). Específicas = {len(etiquetas_especificas)} chars."
            )

        logger.info("  Descripción generada: %s caracteres", len(descripcion))
        logger.info("  Etiquetas totales: %s caracteres", chars_totales)
        logger.info("    └ Canal (fijas): %s chars | Específicas: %s chars",
                    len(ETIQUETAS_CANAL), len(etiquetas_especificas))

        return {"descripcion": descripcion, "etiquetas": etiquetas_finales}

    except Exception as e:
        logger.error("Error parseando respuesta de Claude: %s", e)
        logger.error("Respuesta recibida: %s", resultado)
        raise


def write_to_csv(csv_path: str, row_index: int, descripcion: str, etiquetas: str):
    df = pd.read_csv(csv_path, encoding='utf-8', dtype={'DESCRIPCION': str, 'ETIQUETAS': str})
    df['DESCRIPCION'] = df['DESCRIPCION'].astype(str)
    df['ETIQUETAS'] = df['ETIQUETAS'].astype(str)
    df.at[row_index, 'DESCRIPCION'] = descripcion
    df.at[row_index, 'ETIQUETAS'] = etiquetas
    df.to_csv(csv_path, index=False, encoding='utf-8')
    logger.info(
        "  -> Datos escritos en CSV (fila %s, columnas DESCRIPCION y ETIQUETAS)",
        row_index + 1
    )


def run_seo_generator(row_index: int = 1):
    logger.info("\n" + "=" * 60)
    logger.info("  GENERADOR DE METADATOS SEO PARA YOUTUBE — INICIO")
    logger.info("=" * 60)

    logger.info("\n[1/3] Leyendo título del CSV (fila %s)...", row_index)
    titulo, df_index = get_titulo_from_csv(CSV_PATH, row_index)
    logger.info("  Título: %s", titulo)

    logger.info("\n[2/3] Generando descripción y etiquetas con Sonnet 4.6...")
    metadata = generate_seo_metadata(titulo)

    logger.info("\n[3/3] Escribiendo resultados en CSV...")
    write_to_csv(CSV_PATH, df_index, metadata["descripcion"], metadata["etiquetas"])

    logger.info("\n" + "=" * 60)
    logger.info("  GENERACIÓN COMPLETADA EXITOSAMENTE")
    logger.info("=" * 60)
    logger.info("  Fila procesada:        %s", row_index)
    logger.info("  Archivo CSV:           %s", CSV_PATH)
    logger.info("  Descripción (chars):   %s", len(metadata["descripcion"]))
    logger.info("  Etiquetas (chars):     %s", len(metadata["etiquetas"]))
    logger.info("=" * 60 + "\n")
    logger.info("\n--- PREVIEW DE DESCRIPCIÓN ---")
    logger.info(metadata["descripcion"][:300] + "...")
    logger.info("\n--- PREVIEW DE ETIQUETAS ---")
    logger.info(metadata["etiquetas"][:200] + "...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generador automático de metadatos SEO para YouTube"
    )
    parser.add_argument(
        "--fila", type=int, default=1,
        help="Fila a procesar (1 = primera fila de datos)"
    )
    args = parser.parse_args()

    try:
        run_seo_generator(row_index=args.fila)
    except FileNotFoundError as e:
        logger.error("ERROR: %s", e); sys.exit(1)
    except ValueError as e:
        logger.error("ERROR: %s", e); sys.exit(1)
    except anthropic.AuthenticationError:
        logger.error(
            "ERROR: API Key inválida.\n"
            "  Verifica que la clave en el archivo .env sea correcta.\n"
            "  Obtén una nueva en: https://console.anthropic.com/"
        ); sys.exit(1)
    except anthropic.RateLimitError:
        logger.error(
            "ERROR: Rate limit alcanzado. Espera unos minutos e intenta de nuevo."
        ); sys.exit(1)
    except anthropic.APIStatusError as e:
        logger.error("ERROR de API (%s): %s", e.status_code, e.message); sys.exit(1)
    except Exception as e:
        logger.error("ERROR inesperado: %s", e, exc_info=True); sys.exit(1)