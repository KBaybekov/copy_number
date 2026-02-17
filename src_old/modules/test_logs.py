from logger import get_logger

logger = get_logger(__name__)

logger.info("test")
logger.warning("warning")
logger.error("error")
logger.critical("crit")