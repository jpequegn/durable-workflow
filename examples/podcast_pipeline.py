"""Example: 4-step podcast processing workflow.

Usage:
    uv run wf run examples/podcast_pipeline.py --episode-id ep-123
    uv run wf runs list
    uv run wf status <run_id>
    uv run wf resume <run_id>
"""

from workflow.engine import WorkflowEngine
from workflow.step import step


# ---------------------------------------------------------------------------
# Step functions (pure — no engine awareness)
# ---------------------------------------------------------------------------


def download_audio(episode_id: str) -> str:
    """Simulate downloading an audio file. Returns local path."""
    print(f"  [download_audio] episode={episode_id}")
    return f"/tmp/{episode_id}.mp3"


def transcribe_audio(audio_path: str) -> str:
    """Simulate transcription. Returns transcript text."""
    print(f"  [transcribe_audio] path={audio_path}")
    return "This is the transcript of the episode."


def summarize_text(transcript: str) -> str:
    """Simulate summarisation. Returns a short summary."""
    print(f"  [summarize_text] chars={len(transcript)}")
    return "Short summary of the episode."


def export_to_db(episode_id: str, summary: str) -> bool:
    """Simulate writing the summary to a database."""
    print(f"  [export_to_db] episode={episode_id} summary={summary!r}")
    return True


# ---------------------------------------------------------------------------
# Workflow function
# ---------------------------------------------------------------------------


def process_podcast(episode_id: str) -> bool:
    audio_path = step("download", download_audio, episode_id)
    transcript = step("transcribe", transcribe_audio, audio_path)
    summary    = step("summarize", summarize_text, transcript)
    return       step("export", export_to_db, episode_id, summary)


# ---------------------------------------------------------------------------
# CLI discovery hook — the wf CLI looks for WORKFLOW and INPUT_SCHEMA
# ---------------------------------------------------------------------------

#: The function the engine should run.
WORKFLOW = process_podcast

#: Declares expected CLI flags (name → Python type).  Used by ``wf run``.
INPUT_SCHEMA: dict[str, type] = {
    "episode_id": str,
}
