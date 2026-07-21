import logging
import threading

import pytest

from gui.scripts import log_bridge


def _fresh_logger():
    """The real spatial_risk logger with the console handler installed."""
    log_bridge.clear_log_records()
    log_bridge.install_log_console_handler()
    logger = logging.getLogger("spatial_risk")
    logger.setLevel(logging.DEBUG)
    return logger


@pytest.fixture(autouse=True)
def _reset_log_bridge_state():
    """Isolate the singleton handler: after every test (pass OR fail) clear the
    buffer and unbind any test-bound context, so a partially-failing test can't
    leak a fake context into later tests."""
    yield
    log_bridge.clear_log_records()
    log_bridge.install_log_console_handler().bind_context(None)


def test_info_record_is_captured_with_expected_keys():
    logger = _fresh_logger()
    logger.info("hello info")
    records = log_bridge.log_records.value
    assert any(r["msg"] == "hello info" for r in records)
    rec = next(r for r in records if r["msg"] == "hello info")
    assert set(rec) == {"time", "level", "name", "msg"}
    assert rec["level"] == "INFO"
    assert rec["name"] == "spatial_risk"


def test_debug_record_is_dropped():
    logger = _fresh_logger()
    logger.debug("hidden debug")
    msgs = [r["msg"] for r in log_bridge.log_records.value]
    assert "hidden debug" not in msgs


def test_buffer_is_capped_at_max_records():
    logger = _fresh_logger()
    for i in range(log_bridge.MAX_RECORDS + 25):
        logger.info("line %d", i)
    records = log_bridge.log_records.value
    assert len(records) == log_bridge.MAX_RECORDS
    # Newest retained, oldest dropped.
    assert records[-1]["msg"] == f"line {log_bridge.MAX_RECORDS + 24}"
    assert records[0]["msg"] != "line 0"


def test_set_publishes_a_new_tuple_identity():
    logger = _fresh_logger()
    before = log_bridge.log_records.value
    logger.info("identity check")
    after = log_bridge.log_records.value
    assert isinstance(after, tuple)
    assert after is not before


def test_install_is_idempotent():
    log_bridge.install_log_console_handler()
    log_bridge.install_log_console_handler()
    handlers = [
        h for h in logging.getLogger("spatial_risk").handlers
        if isinstance(h, log_bridge.ReactiveLogHandler)
    ]
    assert len(handlers) == 1


def test_clear_empties_the_buffer():
    logger = _fresh_logger()
    logger.info("something")
    assert log_bridge.log_records.value
    log_bridge.clear_log_records()
    assert log_bridge.log_records.value == tuple()


def test_emit_from_bare_thread_does_not_raise_and_appends():
    logger = _fresh_logger()

    def worker():
        logger.info("from a bare thread")

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    msgs = [r["msg"] for r in log_bridge.log_records.value]
    assert "from a bare thread" in msgs


def test_emit_during_a_render_does_not_subscribe_the_rendering_component():
    """A logging handler must stay invisible to reactive dependency tracking.

    ``emit`` reads ``log_records`` with ``.peek()``, never ``.value``: ``.value``
    goes through ``Reactive.get()``, which registers the reactive in Solara's
    ``thread_local.reactive_used`` and so AUTO-SUBSCRIBES whatever component is
    rendering on this thread. A component that logs during its render would then
    be subscribed to ``log_records`` by that very log call, and the ``.set()``
    right after it would force it to render again, log again, and never stop.

    This pins the invariant on its own — the app also avoids logging during
    render (the evaluation card loads inside a ``use_memo``), so either measure
    alone hides a regression in the other. Here the component logs from its
    render body deliberately: N ordinary renders must stay N renders.
    """
    import reacton
    import solara

    logger = _fresh_logger()
    renders = []

    @solara.component
    def _LoggingComponent(pass_no):
        renders.append(pass_no)
        logger.warning("logged from a render body (pass %d)", pass_no)
        solara.Text(f"pass {pass_no}")

    _, rc = reacton.render(_LoggingComponent(1), handle_error=False)
    try:
        assert renders == [1]
        rc.render(_LoggingComponent(2))  # one ordinary re-render, not a loop
        assert renders == [1, 2]
    finally:
        rc.close()


def test_emit_attaches_bound_context_for_contextless_thread(monkeypatch):
    import threading
    from solara.server import kernel_context

    logger = _fresh_logger()
    handler = log_bridge.install_log_console_handler()
    sentinel = object()
    handler.bind_context(sentinel)

    attached = []
    monkeypatch.setattr(
        kernel_context,
        "set_context_for_thread",
        lambda ctx, thread: attached.append(ctx),
    )

    errors = []

    def worker():
        try:
            logger.info("ctx-bound emit")
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    t = threading.Thread(target=worker)
    t.start()
    t.join()

    assert not errors
    assert sentinel in attached  # bound context attached before the set
    msgs = [r["msg"] for r in log_bridge.log_records.value]
    assert "ctx-bound emit" in msgs
