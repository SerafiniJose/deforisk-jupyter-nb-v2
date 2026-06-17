"""ProjectSession: the runtime aggregate root over an inert ProjectDocument.

Never serialized. The only thing that crosses save/load/worker boundaries is
the frozen ProjectDocument reached via snapshot().
"""

from __future__ import annotations

from typing import Any, Optional

from spatialrisk.document import ProjectDocument


class ProjectSession:
    """Ergonomic, mutable-feeling wrapper over an immutable ProjectDocument.

    Mutations never touch the document in place: each goes through the
    validated `_replace` primitive, which round-trips the document through
    `model_validate` and bumps `doc_version`.
    """

    def __init__(
        self,
        doc: ProjectDocument,
        *,
        store: Any = None,
        estimator_store: Any = None,
        gee: Any = None,
    ) -> None:
        self._doc = doc
        self.doc_version: int = 0
        self.store = store
        self.estimator_store = estimator_store
        self.gee = gee
        # Driver-side ONLY; keyed by model_key; NEVER shipped to workers.
        self.estimator_cache: dict[str, Any] = {}

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    @classmethod
    def from_document(
        cls,
        doc: ProjectDocument,
        *,
        store: Any = None,
        estimator_store: Any = None,
        gee: Any = None,
    ) -> "ProjectSession":
        return cls(doc, store=store, estimator_store=estimator_store, gee=gee)

    def snapshot(self) -> ProjectDocument:
        """Return the current inert document (the crossing-boundary artifact)."""
        return self._doc

    @property
    def project_name(self) -> str:
        return self._doc.project_name
