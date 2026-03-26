"""Step 3 — Dataset tile (stub)."""

import solara


@solara.component
def DatasetTile(project):
    """Dataset configuration step — stub, to be implemented.

    Args:
        project: Reactive holding the current Project (or None).
    """
    with solara.Column(style="gap: 16px;"):
        solara.Markdown("### Step 3 — Dataset")
        solara.Info("Dataset configuration is not yet implemented. This step will allow you to define target and feature variables for model training.")
