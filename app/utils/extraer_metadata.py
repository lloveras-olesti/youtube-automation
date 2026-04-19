#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
EXTRACTOR DE METADATA DEL CALENDARIO
Lee el CSV calendario.csv y extrae la primera fila de datos
"""

import pandas as pd
import json
import os
import sys
from app.config import settings

# Configuración
DATA_PATH = os.environ.get("DATA_PATH", "/app/data")
CSV_FILE = str(settings.calendario_path)


def main():
    try:
        # Cargar CSV
        df = pd.read_csv(CSV_FILE, encoding='utf-8')

        # Verificar que hay datos
        if len(df) == 0:
            print(json.dumps(
                {"error": "El calendario está vacío"}), file=sys.stderr)
            sys.exit(1)

        # Extraer datos de la primera fila (índice 0)
        # Columnas: FECHA, TÍTULO, SALMO, TEXTO, INTRO, DESCRIPCION, ETIQUETAS
        row = df.iloc[0]
        
        data = {
            "fecha": str(row['FECHA']),           # FECHA
            "titulo": str(row['TÍTULO']),         # TÍTULO
            "salmo": str(row['SALMO']),           # SALMO
            "texto": str(row['TEXTO']),           # TEXTO
            "intro": str(row['INTRO']),           # INTRO
            "descripcion": str(row['DESCRIPCION']) if pd.notna(row['DESCRIPCION']) else '',  # DESCRIPCION
            "etiquetas": str(row['ETIQUETAS']) if pd.notna(row['ETIQUETAS']) else ''         # ETIQUETAS
        }

        # Validar que hay datos
        if not data['titulo'] or data['titulo'] == 'nan':
            print(json.dumps(
                {"error": "No hay título en la primera fila"}), file=sys.stderr)
            sys.exit(1)

        # Imprimir JSON para que n8n lo capture
        print(json.dumps(data, ensure_ascii=False))

    except FileNotFoundError:
        print(json.dumps(
            {"error": f"No se encuentra el archivo: {CSV_FILE}"}), file=sys.stderr)
        sys.exit(1)
    except KeyError as e:
        print(json.dumps(
            {"error": f"Columna faltante en CSV: {e}"}), file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
