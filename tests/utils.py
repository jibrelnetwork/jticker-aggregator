from jticker_aggregator.logging import _configure_logging


def test_configure_logging():
    _configure_logging('INFO')
    _configure_logging('DEBUG')
