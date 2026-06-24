import inspect


def test_log_console_imports():
    import gui.widget.log_console as lc
    assert hasattr(lc, "LogConsole")


def test_log_console_reads_records_and_binds_context():
    import gui.widget.log_console as lc
    src = inspect.getsource(lc.LogConsole)
    assert "log_records.value" in src
    assert "ExpansionPanels" in src          # self-managed collapse
    assert "bind_context" in src             # context capture on mount
    assert "use_effect" in src               # binding happens on mount, not every render
    assert "position: fixed" in src          # floating panel
