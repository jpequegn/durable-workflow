"""Example: 4-step podcast processing workflow.

This example will be wired up to the real engine in a future issue.
For now it shows the intended API shape.

Usage (once the engine is implemented):
    uv run wf run examples/podcast_pipeline.py --episode-id ep-123
"""

# from workflow import workflow, step, WorkflowEngine


def download_audio(episode_id: str) -> str:
    """Simulate downloading an audio file. Returns local path."""
    print(f"[download_audio] episode={episode_id}")
    return f"/tmp/{episode_id}.mp3"


def transcribe_audio(audio_path: str) -> str:
    """Simulate transcription. Returns transcript text."""
    print(f"[transcribe_audio] path={audio_path}")
    return "This is the transcript of the episode."


def summarize_text(transcript: str) -> str:
    """Simulate summarisation. Returns a short summary."""
    print(f"[summarize_text] chars={len(transcript)}")
    return "Short summary of the episode."


def export_to_db(episode_id: str, summary: str) -> bool:
    """Simulate writing the summary to a database."""
    print(f"[export_to_db] episode={episode_id} summary={summary!r}")
    return True


# @workflow
# def process_podcast(episode_id: str):
#     audio_path  = step("download",    download_audio,   episode_id)
#     transcript  = step("transcribe",  transcribe_audio, audio_path)
#     summary     = step("summarize",   summarize_text,   transcript)
#     return       step("export",       export_to_db,     episode_id, summary)


if __name__ == "__main__":
    # Placeholder: run the steps sequentially (no durability yet)
    episode_id = "ep-123"
    audio_path = download_audio(episode_id)
    transcript = transcribe_audio(audio_path)
    summary = summarize_text(transcript)
    result = export_to_db(episode_id, summary)
    print(f"Done: {result}")
