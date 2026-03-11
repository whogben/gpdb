import pytest

try:
    from gpdb.admin import entry

    HAS_ADMIN = True
except ImportError:
    HAS_ADMIN = False

admin_only = pytest.mark.skipif(not HAS_ADMIN, reason="admin module not installed")


@admin_only
def test_entry_main(capsys):
    entry.main()
    captured = capsys.readouterr()
    assert captured.out == "hello world\n"
