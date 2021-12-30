import logging.config


class Logger:
    """This class configurate logging and holds logger instance"""
    def __init__(self, log_config: dict) -> None:
        logging.config.dictConfig(log_config)
        self.logger = logging.getLogger(__name__)
