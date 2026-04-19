#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
=================================================
CONFIGURACIÓN CENTRALIZADA DEL PROYECTO CANAL RELI
=================================================
Gestiona rutas, variables de entorno y mapeos de prompts.

Ubicación: /app/config.py

USO:
    from app.config import settings, PROMPT_STYLES, PROMPT_INTROS
    
    # Acceder a rutas
    calendario = settings.calendario_path
    
    # Acceder a mapeos
    estilo_file = PROMPT_STYLES["formal"]
    
    # Leer prompt
    content = settings.read_prompt("estilos", "formal")
"""

import os
from pathlib import Path
from typing import Dict

# ============================================
# CONFIGURACIÓN CENTRALIZADA
# ============================================

class Settings:
    """Configuración centralizada del proyecto"""
    
    def __init__(self):
        # ============================================
        # APIS EXTERNAS
        # ============================================
        self.anthropic_api_key = os.environ.get("ANTHROPIC_API_KEY")
        self.youtube_api_key = os.environ.get("YOUTUBE_API_KEY")
        
        if not self.anthropic_api_key:
            raise ValueError("ANTHROPIC_API_KEY no está configurada en .env")
        
        # ============================================
        # URLS
        # ============================================
        self.canal_url = os.environ.get(
            "CANAL_URL",
            "https://www.youtube.com/channel/UCdvIFcgn_ci3pddGGqhLbXw?sub_confirmation=1"
        )
        
        # ============================================
        # PATHS BASE
        # ============================================
        self.data_path = Path(os.environ.get("DATA_PATH", "/app/data"))
        self.prompts_path = Path("/app/prompts")
        self.config_path = Path("/app/config")
        self.runtime_path = Path(os.environ.get("RUNTIME_PATH", "/app/runtime"))
        
        # ============================================
        # PATHS DE DATOS
        # ============================================
        self.input_path = self.data_path / "input"
        self.temp_path = self.data_path / "temp"
        self.output_path = self.data_path / "output"
        self.logs_path = self.data_path / "logs"
        
        # ============================================
        # ARCHIVOS ESPECÍFICOS
        # ============================================
        self.calendario_path = self.input_path / "calendario.csv"
        self.config_calendario_path = self.config_path / "config_calendario.yaml"
        self.historico_path = self.runtime_path / "historico_videos.csv"
        self.insights_path = self.runtime_path / "insights_acumulados.json"
        self.referencias_path = self.runtime_path / "referencias_canales.json"
        self.token_path = self.data_path / "youtube_token.json"
        self.estado_recursos_path = self.input_path / "estado_recursos.json"
        
        # ============================================
        # PATHS DE RECURSOS
        # ============================================
        self.recursos_path = self.input_path / "recursos"
        self.videos_loop_path = self.recursos_path / "videos-loop"
        self.musica_path = self.recursos_path / "musica"
        
        # ============================================
        # PATHS TEMPORALES
        # ============================================
        self.temp_audio_path = self.temp_path / "audio"
        self.temp_video_path = self.temp_path / "video"
        self.temp_subtitles_path = self.temp_path / "subtitles"
        
        # ============================================
        # PATHS DE OUTPUT
        # ============================================
        self.output_videos_path = self.output_path / "videos"
        self.output_metadata_path = self.output_path / "metadata"
        
        # ============================================
        # MODELOS CLAUDE
        # ============================================
        self.claude_haiku_model = os.environ.get(
            "CLAUDE_HAIKU_MODEL", "claude-haiku-4-5-20251001"
        )
        self.claude_sonnet_model = os.environ.get(
            "CLAUDE_SONNET_MODEL", "claude-sonnet-4-6"
        )
        
        # ============================================
        # PARÁMETROS DE GENERACIÓN
        # ============================================
        self.script_length = int(os.environ.get("SCRIPT_LENGTH", "5000"))
        self.video_resolution = os.environ.get("VIDEO_RESOLUTION", "1920x1080")
        self.whisper_model_size = os.environ.get("WHISPER_MODEL_SIZE", "small")
        self.whisper_language = os.environ.get("WHISPER_LANGUAGE", "es")
        
    def read_prompt(self, category: str, name: str) -> str:
        """
        Lee un archivo de prompt.
        
        Args:
            category: Carpeta (estilos, intros, especificos)
            name: Nombre del archivo sin extensión
        
        Returns:
            Contenido del archivo
        
        Example:
            >>> content = settings.read_prompt("estilos", "formal")
        """
        path = self.prompts_path / category / f"{name}.md"
        if not path.exists():
            raise FileNotFoundError(f"Prompt no encontrado: {path}")
        return path.read_text(encoding="utf-8")
    
    def get_temp_file(self, category: str, filename: str) -> Path:
        """
        Genera path a archivo temporal.
        
        Args:
            category: audio, video, subtitles
            filename: Nombre del archivo
        
        Returns:
            Path completo
        """
        category_path = self.temp_path / category
        category_path.mkdir(parents=True, exist_ok=True)
        return category_path / filename
    
    def get_output_file(self, category: str, filename: str) -> Path:
        """
        Genera path a archivo de output.
        
        Args:
            category: videos, metadata
            filename: Nombre del archivo
        
        Returns:
            Path completo
        """
        category_path = self.output_path / category
        category_path.mkdir(parents=True, exist_ok=True)
        return category_path / filename


# ============================================
# MAPEOS DE PROMPTS
# ============================================

# IMPORTANTE: Si cambias los nombres de los archivos .md en prompts/,
# actualiza estos diccionarios.

PROMPT_STYLES: Dict[str, str] = {
    # Mapeo: valor en CSV → nombre de archivo .md (sin extensión)
    "formal": "formal",       # prompts/estilos/formal.md
    "cercano": "cercano",     # prompts/estilos/cercano.md
    "reflexivo": "reflexivo"  # prompts/estilos/reflexivo.md
}

PROMPT_INTROS: Dict[str, str] = {
    "intro-rapida": "intro-rapida",
    "intro-contextual": "intro-contextual",
    "intro-emocional": "intro-emocional"
}

# Archivo de prompt maestro (base para generación de guión)
PROMPT_MASTER_FILE = "oracion-base"  # prompts/especificos/oracion-base.md


# ============================================
# INSTANCIA GLOBAL
# ============================================

settings = Settings()
