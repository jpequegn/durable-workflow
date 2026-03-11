"""Example: 4-step podcast processing workflow.

Demonstrates a realistic durable pipeline:

    download → transcribe (with retries) → summarize → save

Each step is a pure function. The engine handles persistence, caching, and
resumability — step functions have no awareness of any of that.

Usage
-----
    # Run from scratch
    uv run wf run examples/podcast_pipeline.py --episode-id ep-123

    # Check status
    uv run wf status <run_id>

    # If it crashed mid-way, resume (steps already done are skipped)
    uv run wf resume <run_id> --workflow-file examples/podcast_pipeline.py

    # Full trace
    uv run wf runs inspect <run_id> --show-output

Before / after durability
--------------------------
Before (naive):
    audio = download_audio(episode_id)
    transcript = transcribe_audio(audio)      # ← process crashes here
    summary = summarize_text(transcript)      # re-runs download + transcribe on restart
    save_to_database(episode_id, summary)

After (durable):
    audio_path = step("download",    download_audio,   episode_id)
    transcript = step("transcribe",  transcribe_audio, audio_path, max_retries=2)
    summary    = step("summarize",   summarize_text,   transcript)
    _          = step("save",        save_to_database, episode_id, summary)
    # crash at "summarize" → restart → download + transcribe are cache hits
"""

from __future__ import annotations

import time

from workflow.step import step


# ---------------------------------------------------------------------------
# Step functions — pure, no engine awareness
# ---------------------------------------------------------------------------


def download_audio(episode_id: str) -> str:
    """Simulate fetching an audio file from a remote URL.

    In a real pipeline this would be an HTTP download or S3 copy.
    Returns the local file path.
    """
    print(f"  [download_audio] Fetching episode {episode_id!r} …")
    time.sleep(0.01)  # simulate I/O
    path = f"/tmp/podcasts/{episode_id}.mp3"
    print(f"  [download_audio] Saved to {path}")
    return path


def transcribe_audio(audio_path: str) -> str:
    """Simulate speech-to-text transcription.

    In a real pipeline this would call Whisper or an external API.
    Returns the full transcript as a string.
    """
    print(f"  [transcribe_audio] Transcribing {audio_path} …")
    time.sleep(0.01)  # simulate GPU/API latency
    transcript = (
        "Welcome to this episode. Today we discuss durable workflow engines "
        "and why every serious agent system eventually needs one. "
        "The key insight is that steps should be idempotent: same inputs, "
        "same outputs, cached forever."
    )
    print(f"  [transcribe_audio] Transcript: {len(transcript)} chars")
    return transcript


def summarize_text(transcript: str) -> str:
    """Simulate LLM summarisation.

    In a real pipeline this would call Ollama or the OpenAI API.
    Returns a short summary paragraph.
    """
    print(f"  [summarize_text] Summarising {len(transcript)}-char transcript …")
    time.sleep(0.01)  # simulate LLM latency
    # Naive extractive summary: first sentence.
    summary = transcript.split(".")[0].strip() + "."
    print(f"  [summarize_text] Summary: {summary!r}")
    return summary


def save_to_database(episode_id: str, summary: str) -> dict:
    """Simulate persisting the result to a database.

    Returns a dict with the persisted record metadata.
    """
    print(f"  [save_to_database] Saving episode={episode_id!r} …")
    record = {
        "episode_id": episode_id,
        "summary": summary,
        "saved": True,
    }
    print(f"  [save_to_database] Saved: {record}")
    return record


# ---------------------------------------------------------------------------
# Workflow function
# ---------------------------------------------------------------------------


def process_podcast(episode_id: str) -> dict:
    """4-step durable podcast pipeline.

    Steps:
        download    — fetch audio file (cached by episode_id)
        transcribe  — speech-to-text with up to 2 retries
        summarize   — LLM summary of transcript
        save        — persist episode_id + summary to DB

    If the process crashes at any step, resume() replays completed steps
    from cache and continues from the failure point.
    """
    audio_path = step("download",   download_audio,    episode_id)
    transcript = step("transcribe", transcribe_audio,  audio_path, max_retries=2, base_delay=0.1)
    summary    = step("summarize",  summarize_text,    transcript)
    record     = step("save",       save_to_database,  episode_id, summary)
    return record


# ---------------------------------------------------------------------------
# CLI discovery hooks
# ---------------------------------------------------------------------------

WORKFLOW = process_podcast

INPUT_SCHEMA: dict[str, type] = {
    "episode_id": str,
}
