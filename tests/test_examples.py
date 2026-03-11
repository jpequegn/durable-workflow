"""Tests for the example workflows (issue #4).

Verifies that each example workflow:
- Runs end-to-end successfully.
- Survives a simulated crash at any step and resumes correctly.
- Never re-executes a completed step after a crash.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from workflow.engine import WorkflowEngine
from workflow.step import step


# ---------------------------------------------------------------------------
# Podcast pipeline
# ---------------------------------------------------------------------------


class TestPodcastPipeline:
    def test_runs_end_to_end(self, tmp_path: Path) -> None:
        from examples.podcast_pipeline import process_podcast

        with WorkflowEngine(db_path=tmp_path / "wf.db") as engine:
            run_id = engine.run(process_podcast, episode_id="ep-test")
            assert engine.store.get_run(run_id).status == "completed"

    def test_returns_saved_record(self, tmp_path: Path) -> None:
        from examples.podcast_pipeline import process_podcast

        with WorkflowEngine(db_path=tmp_path / "wf.db") as engine:
            engine.run(process_podcast, episode_id="ep-42")
            status = engine.status(engine.store.list_runs(limit=1)[0].id)
            assert status.is_completed

    def test_four_steps_persisted(self, tmp_path: Path) -> None:
        from examples.podcast_pipeline import process_podcast

        with WorkflowEngine(db_path=tmp_path / "wf.db") as engine:
            run_id = engine.run(process_podcast, episode_id="ep-steps")
            steps = engine.store.get_steps(run_id)

        assert {s.step_name for s in steps} == {"download", "transcribe", "summarize", "save"}
        assert all(s.status == "completed" for s in steps)

    def test_crash_at_summarize_resumes_without_redownload(self, tmp_path: Path) -> None:
        """Crash at step 3 (summarize); resume should not re-run download or transcribe."""
        from examples.podcast_pipeline import (
            download_audio,
            process_podcast,
            save_to_database,
            summarize_text,
            transcribe_audio,
        )

        call_counts: dict[str, int] = {}

        def counting(name: str, fn):
            def wrapper(*args, **kwargs):
                call_counts[name] = call_counts.get(name, 0) + 1
                return fn(*args, **kwargs)
            return wrapper

        def crashing_pipeline(episode_id: str) -> dict:
            audio_path = step("download",   counting("download",   download_audio),   episode_id)
            transcript = step("transcribe", counting("transcribe", transcribe_audio), audio_path)
            _ = step("summarize", lambda t: (_ for _ in ()).throw(RuntimeError("crash")), transcript)
            return step("save", counting("save", save_to_database), episode_id, "s")

        with WorkflowEngine(db_path=tmp_path / "wf.db") as engine:
            with pytest.raises(RuntimeError, match="crash"):
                run_id = engine.run(crashing_pipeline, episode_id="ep-crash")

            runs = engine.store.list_runs(limit=1)
            run_id = runs[0].id
            assert call_counts == {"download": 1, "transcribe": 1}

            # Now resume with the fixed pipeline (no crash)
            def fixed_pipeline(episode_id: str) -> dict:
                audio_path = step("download",   counting("download",   download_audio),   episode_id)
                transcript = step("transcribe", counting("transcribe", transcribe_audio), audio_path)
                summary    = step("summarize",  counting("summarize",  summarize_text),   transcript)
                return       step("save",       counting("save",       save_to_database), episode_id, summary)

            engine.register(fixed_pipeline)
            # Patch the registry to map the original name
            engine._registry["crashing_pipeline"] = fixed_pipeline

            engine.resume(run_id)

        # download and transcribe: still exactly 1 (cache hits on resume)
        assert call_counts["download"] == 1
        assert call_counts["transcribe"] == 1
        # summarize and save: ran once during resume
        assert call_counts["summarize"] == 1
        assert call_counts["save"] == 1

    def test_transcribe_retries_on_failure(self, tmp_path: Path) -> None:
        """transcribe step has max_retries=2; transient failures should be retried."""
        from examples.podcast_pipeline import download_audio, save_to_database, summarize_text

        call_count = {"n": 0}

        def flaky_transcribe(audio_path: str) -> str:
            call_count["n"] += 1
            if call_count["n"] < 3:
                raise ConnectionError("API timeout")
            return "transcript"

        def pipeline(episode_id: str) -> dict:
            audio = step("download",   download_audio,    episode_id)
            text  = step("transcribe", flaky_transcribe,  audio, max_retries=2, base_delay=0.0)
            summ  = step("summarize",  summarize_text,    text)
            return step("save",        save_to_database,  episode_id, summ)

        with patch("workflow.step.time.sleep"):
            with WorkflowEngine(db_path=tmp_path / "wf.db") as engine:
                run_id = engine.run(pipeline, episode_id="ep-retry")
                assert engine.store.get_run(run_id).status == "completed"
                # 3 rows for transcribe: attempt 0 failed, 1 failed, 2 succeeded
                transcribe_rows = [
                    s for s in engine.store.get_steps(run_id) if s.step_name == "transcribe"
                ]
                assert len(transcribe_rows) == 3
                assert transcribe_rows[-1].status == "completed"


# ---------------------------------------------------------------------------
# ETL data pipeline
# ---------------------------------------------------------------------------


class TestDataPipeline:
    def test_runs_end_to_end(self, tmp_path: Path) -> None:
        from examples.data_pipeline import etl_pipeline

        with WorkflowEngine(db_path=tmp_path / "wf.db") as engine:
            run_id = engine.run(
                etl_pipeline,
                source_url="https://example.com/data.csv",
                table="staging",
            )
            assert engine.store.get_run(run_id).status == "completed"

    def test_four_steps_persisted(self, tmp_path: Path) -> None:
        from examples.data_pipeline import etl_pipeline

        with WorkflowEngine(db_path=tmp_path / "wf.db") as engine:
            run_id = engine.run(etl_pipeline, source_url="http://x.com/d.csv", table="t")
            steps = engine.store.get_steps(run_id)

        assert {s.step_name for s in steps} == {"download", "transform", "load", "notify"}

    def test_resume_after_load_crash(self, tmp_path: Path) -> None:
        from examples.data_pipeline import download_data, notify, transform_data

        call_counts: dict[str, int] = {}

        def counting(name, fn):
            def w(*a, **k):
                call_counts[name] = call_counts.get(name, 0) + 1
                return fn(*a, **k)
            return w

        def crashing_etl(source_url: str, table: str) -> str:
            raw     = step("download",  counting("download",  download_data),  source_url)
            records = step("transform", counting("transform", transform_data), raw)
            _       = step("load",      lambda r, t: (_ for _ in ()).throw(RuntimeError("db down")), records, table)
            return    step("notify",    counting("notify",    notify),          0)

        with WorkflowEngine(db_path=tmp_path / "wf.db") as engine:
            with pytest.raises(RuntimeError, match="db down"):
                engine.run(crashing_etl, source_url="http://x.com", table="t")

            run_id = engine.store.list_runs(limit=1)[0].id

            def fixed_etl(source_url: str, table: str) -> str:
                from examples.data_pipeline import load_to_db
                raw     = step("download",  counting("download",  download_data),  source_url)
                records = step("transform", counting("transform", transform_data), raw)
                rows    = step("load",      counting("load",      load_to_db),     records, table)
                return    step("notify",    counting("notify",    notify),          rows)

            engine._registry["crashing_etl"] = fixed_etl
            engine.resume(run_id)

        assert call_counts["download"] == 1   # cache hit
        assert call_counts["transform"] == 1  # cache hit
        assert call_counts["load"] == 1       # ran on resume
        assert call_counts["notify"] == 1     # ran on resume


# ---------------------------------------------------------------------------
# Flaky steps example
# ---------------------------------------------------------------------------


class TestFlakyPipeline:
    def test_runs_with_retries(self, tmp_path: Path) -> None:
        from examples.flaky_steps import flaky_pipeline

        with WorkflowEngine(db_path=tmp_path / "wf.db") as engine:
            run_id = engine.run(flaky_pipeline, seed=42)
            assert engine.store.get_run(run_id).status == "completed"
            fetch_rows = [
                s for s in engine.store.get_steps(run_id) if s.step_name == "fetch"
            ]
            # fail_times=2 → attempt 0 failed, 1 failed, 2 succeeded
            assert len(fetch_rows) == 3
            assert fetch_rows[-1].status == "completed"
