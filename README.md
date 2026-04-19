# Automated Long-Form YouTube Content Pipeline

End-to-end automation system for producing and publishing long-form YouTube videos. Given a content calendar, the pipeline generates script, synthesizes voice locally (XTTS v2 on GPU), transcribes subtitles (Whisper), composes thumbnails (Pillow), renders the final video (FFmpeg), and uploads to YouTube — all orchestrated via a JSON-defined workflow runner.

Thumbnail automation was tested using ComfyUI (SDXL) for background images and Pillow for text composition, using set prompts to generate images locally with certain composition. Generated images resulted inconsistent in quality and composition, so it was decided to use a manual approach for thumbnail generation, using fixed high-quality images and concept rotation.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Daily Video Pipeline](#daily-video-pipeline)
- [Troubleshooting](#troubleshooting)

---

## Features

- **Analytics & Strategy**: Historical and competitor video analysis via YouTube API + Claude
- **Calendar Generation**: Automated 30-day planning driven by CTR/view metrics and rotation algorithms
- **Script Generation**: Structured long-form scripts with configurable styles and intros (Claude API)
- **Local Voice Synthesis (XTTS v2)**: Realistic GPU-accelerated TTS running entirely on-host
- **Automatic Transcription**: Synchronized SRT subtitles via Whisper
- **Thumbnail Generation**: Dynamic text sizing/coloring with Pillow
- **Video Rendering**: FFmpeg pipeline mixing looping video layers, voice, background music, and burned subtitles
- **SEO Optimization**: Auto-generated titles, descriptions, and tags
- **Orchestrated Workflows**: JSON-defined pipeline graphs executed via a generic `run_pipeline.py` runner

---

## Architecture

```
┌─────────────────────────────────────────────────-┐
│               WINDOWS HOST                       │
│                                                  │
│  ┌──────────────────┐      ┌───────────────────┐ │
│  │ run_pipeline.py  │◄────►│canal-reli (Docker)│ │
│  │ (Orchestrator)   │      │(Whisper, FFmpeg,  │ │
│  └───────┬──────────┘      │ Claude API)       │ │
│          │                 └───────────────────┘ │
│          ▼                                       │
│  ┌──────────────────┐      ┌───────────────────┐ │
│  │ generar_audio.py │      │ generar_portada   │ │
│  │ (XTTS v2 / GPU)  │      │ (Pillow)          │ │
│  └──────────────────┘      └───────────────────┘ │
└────────────────────────────────────────────────--┘
```

**Stack:**
- **Pipeline Runner**: Pure Python orchestrator based on JSON workflow definitions
- **Python 3.11/3.12**: All generation and pipeline logic
- **Claude Sonnet**: Script generation, competitor analysis, thumbnail layout
- **XTTS v2**: Local voice synthesis (GPU, Windows host)
- **Pillow**: Thumbnail text composition and styling
- **Whisper**: Audio transcription → SRT
- **FFmpeg**: Video assembly (layers, audio mix, subtitle burn)
- **Docker**: Containerized runtime for the non-GPU pipeline steps

**Execution entry points (Windows):**

| Script | Description |
|--------|-------------|
| `generate.bat` | Runs the video pipeline for row 1 of `calendario.csv` |
| `generate_batch.bat` | Iterates through all calendar rows; since `limpiar_despues_upload.py` removes the first row after each run, it processes the full queue sequentially |
| `monthly_routine.bat` | Runs the monthly analytics pipeline to refresh competitor data and generate a new `calendario.csv` |

---

## Requirements

### System
- Windows 10/11 with WSL2
- Docker Desktop
- 16 GB RAM minimum
- NVIDIA GPU with CUDA support (for XTTS v2)
- Python 3.10+ on Windows host

### External APIs
- Anthropic Claude API
- YouTube Data API v3 + OAuth 2.0 credentials

---

## Installation

### 1. Clone

```bash
git clone <repo-url> canal-reli
cd canal-reli
```

### 2. Environment Variables

```bash
cp .env.example .env
# Fill in your API keys
```

Key variables:
```env
ANTHROPIC_API_KEY=sk-ant-...
YOUTUBE_API_KEY=your_key
CANAL_URL=https://www.youtube.com/channel/YOUR_ID
```

### 3. YouTube OAuth Credentials

Download `youtube_client_secrets.json` from Google Cloud Console (OAuth 2.0 Client IDs) and place it at `config/youtube_client_secrets.json`. The first run of `subir_video.py` will open a browser auth flow and save the token locally.

### 4. TTS Environment (Windows Host)

XTTS v2 runs on the host to access the GPU directly:

```powershell
python -m venv tts-venv
.\tts-venv\Scripts\activate
pip install -r app/generators/requirements-tts.txt
```

### 5. Download XTTS v2 Model

```powershell
.\tts-venv\Scripts\activate
tts --model_name tts_models/multilingual/multi-dataset/xtts_v2 --download
```

This downloads ~3.5 GB into `tts-model/` (gitignored).

### 6. Voice Reference File

XTTS v2 requires a reference audio clip to clone the target voice. Provide a clean 6–10 second WAV recording and place it at:

```
data/input/referencia.wav
```

Requirements: mono or stereo, 22050+ Hz sample rate, no background noise.

### 7. Populate Visual Assets

The pipeline uses rotating video loops and categorized thumbnail backgrounds. Fill the following directories with your own media:

```
data/input/recursos/musica/          # Background music: music1.WAV, music2.WAV, ...
data/input/recursos/videos-loop/     # 5 folders (foto1–foto5), each with numbered .mp4 files
data/input/recursos/portadas/        # One subfolder per thumbnail theme, each with images
```

After populating, update `data/input/estado_recursos.json` with the actual file counts per folder:

```json
{
  "foto1": { "ultimo_usado": 0, "total": 26 },
  "foto2": { "ultimo_usado": 0, "total": 21 },
  ...
}
```

### 8. Build and Start Docker

```bash
# From the project root
docker-compose build
docker-compose up -d
```

---

## Configuration

### `config/config_calendario.yaml`
Controls calendar generation: number of days, testing ratio, topic rotation frequencies.

### `prompts/`
All Claude prompts are modular Markdown files:
- `estilos/` — tone and personality blocks
- `intros/` — intro style variants
- `especificos/` — the master structural prompt

Built for a religious content channel, but the architecture is domain-agnostic: adapting it to any long-form niche requires only replacing the `prompts/` templates, updating `config/config_calendario.yaml`, and changing some aspects of the pipeline scripts, mainly in `generar_guion.py`. The prompt structure should also be updated. The ones in the 'estilos' folder are specific to the religious niche, and their only goal is to ensure text style variety. 'intros' folder content should be updated with prompts that consistently generate introduction patterns that match the channel's style, tone and niche. `config/config_calendario.yaml` should be updated with this information, only if new files are added or file names are changed. Most likely, other niches will not need the 'estilos' folder. Information should be updated in the `prompts/especificos/oracion-base.md` file, which is used by `generar_guion.py` to generate the script.

### `workflows/`
JSON graphs defining step execution order and dependencies. Each step maps to a Python script and specifies whether it runs inside Docker or on the host.

---

## Usage

### Monthly Analytics + Calendar Generation

```bash
python app/pipeline/run_pipeline.py --pipeline workflows/mensual-pipeline.json
```

or execute monthly_routine.bat

After first 30 videos, the pipeline activated by `monthly_routine.py` will gather data from the youtube channel (after proper configuration), from the competence channels defined in `canales_referencia_youtube` parameter (in `config/config_calendario.yaml`), and will generate a new content calendar based on the historical data and the competence data. Metrics used are: CTR and views for own channel, and views for competence channels. Based on these data, Claude will identify certain patterns and will use them as parameters to generate a new content calendar. This system forces the calendar to include videos very similar to the most succesful ones generated by the competence, as well as videos significantly different from the ones that tend to work in the channel. May overlap. Referenced in calendar as `VARIACION_COMPETENCIA`. Testing videos are referenced as `ES_TESTING`.

### Daily Video Generation

```bash
python app/pipeline/run_pipeline.py --pipeline workflows/video-pipeline.json --fila 1
```

or execute generate.bat

Runs the full video pipeline for row 1 of `calendario.csv` in topological order.

Executing generate_batch.bat executes pipeline for each row of the calendar. Since app\utils\limpiar_despues_upload.py deletes first row of calendar.csv, it will run continuously. Calendar log goes to data/logs/calendario.

### Resume from a Specific Step

```bash
python app/pipeline/run_pipeline.py --pipeline workflows/video-pipeline.json --fila 1 --desde subir-video
```

### Flags

| Flag | Description |
|------|-------------|
| `--dry-run` | Simulate execution without side effects |
| `--no-upload` | Skip YouTube upload step |
| `--desde <step>` | Resume from a named step |

---

## Project Structure

```
canal-reli/
├── app/
│   ├── generators/
│   │   ├── generar_guion.py          # Script generation (Claude)
│   │   ├── generar_audio.py          # TTS synthesis (XTTS v2, host)
│   │   ├── generar_subtitulos.py     # Whisper transcription
│   │   ├── generar_calendario.py     # Calendar logic
│   │   ├── generar_portada.py        # Thumbnail assembly (Pillow)
│   │   ├── generar_seo.py            # SEO metadata generation
│   │   ├── aprendizaje_mensual.py    # Monthly analytics + learning
│   │   └── obtener_referencias.py    # Competitor analysis
│   ├── pipeline/
│   │   ├── run_pipeline.py           # Main orchestrator
│   │   └── generar_video.py          # FFmpeg video assembly
│   └── utils/                        # YouTube uploader, cleanup, layout utils
├── config/
│   └── config_calendario.yaml        # Calendar and generation parameters
├── data/
│   ├── input/
│   │   ├── recursos/
│   │   │   ├── musica/               # Background audio (not in repo)
│   │   │   ├── portadas/             # Thumbnail asset categories (not in repo)
│   │   │   └── videos-loop/          # Loop video layers (not in repo)
│   │   ├── calendario.csv            # Video queue (headers only in repo)
│   │   └── estado_recursos.json      # Asset rotation state
│   ├── output/                       # Final videos, metadata (not in repo)
│   ├── temp/                         # Intermediate files (not in repo)
│   └── logs/                         # Per-step execution logs (not in repo)
├── docs/
├── prompts/
│   ├── estilos/
│   ├── intros/
│   └── especificos/
├── workflows/                        # JSON pipeline definitions
├── .env.example
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

---

## Daily Video Pipeline

Steps executed by `video-pipeline.json` in dependency order:

1. **Script** (`generar_guion.py` — Docker) — reads `calendario.csv` row, combines prompts, calls Claude API
2. **Audio** (`generar_audio.py` — host) — XTTS v2 voice synthesis on GPU → `1.wav`
3. **Subtitles** (`generar_subtitulos.py` — Docker) — Whisper transcription → `1_upper.srt`
4. **Thumbnail layout** (`formatear_portadas.py` — Docker) — Claude determines text sizes and colors
5. **Thumbnail render** (`generar_portada.py` — host) — Get image + Pillow text overlay
6. **Video assembly** (`generar_video.py` — Docker) — FFmpeg mixes 5-layer video loop, voice, music, subtitles
7. **SEO** (`generar_seo.py` — Docker) — generates title, description, tags
8. **Upload** (`subir_video.py` — Docker) — YouTube API upload with full metadata
9. **Cleanup** (`limpiar_despues_upload.py`) — removes temp files, archives outputs

---

## Troubleshooting

**YouTube token expired**
Delete the saved token file (`config/youtube_token.json` or similar) and re-run `subir_video.py` to trigger a new OAuth browser flow.

**CUDA error in generar_audio**
Verify that `tts-venv` was installed with the correct Torch version for your GPU drivers.

**Audio/video duration mismatch**
The video duration is derived from TTS output length plus a padding margin. If clipping occurs, check your FFmpeg version or run the duration test function in `generar_video.py` directly.

**ModuleNotFoundError in run_pipeline.py**
Run the orchestrator from the Windows host Python environment (or `tts-venv`) when the pipeline includes `host_command` steps. Docker-only steps can be triggered from inside the container.

---

## License

MIT
