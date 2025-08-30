# pylint: disable=C0114
import logging


def configure_logging(debug_mode: bool) -> None:
    """
    Configures logging based on the provided debug_mode.
    Parameters:
        debug_mode (bool): Whether to enable debug logging.
    Returns:
        None
    """
    log_level = logging.DEBUG if debug_mode else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        encoding="utf-8",
    )
