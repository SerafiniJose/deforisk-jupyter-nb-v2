"""Generated predictions must be published to reactive UI subscribers."""

import solara


class _Project:
    def __init__(self, project_name="demo", predictions=None):
        self.project_name = project_name
        self.predictions = predictions if predictions is not None else {}

    def model_copy(self):
        return _Project(self.project_name, self.predictions)


def test_completed_inference_republishes_project(monkeypatch):
    """A finished run republishes the project so subscribers re-render."""
    from gui.scripts import inference_runner
    from gui.tile import inference_tile

    captured = _Project()
    project = solara.reactive(captured, equals=lambda a, b: a is b)

    def fake_run_inference(p, model_key, dataset_key, name=None, mask_feature=None):
        p.predictions[name] = object()

    monkeypatch.setattr(inference_runner, "run_inference", fake_run_inference)
    previous_jobs = inference_tile.inference_jobs.value
    try:
        inference_tile.inference_jobs.set(
            [
                {"id": "job-1", "status": "running", "error": None},
            ]
        )

        inference_tile._run_inference(
            "job-1",
            "glm_model",
            "dataset",
            captured,
            "prediction",
            project,
        )

        assert project.value is not captured
        assert "prediction" in project.value.predictions
        assert inference_tile.inference_jobs.value[0]["status"] == "completed"
    finally:
        inference_tile.inference_jobs.set(previous_jobs)
