import logging.config


class Logger:
    def __init__(self, log_config: dict) -> None:
        logging.config.dictConfig(log_config)
        self.logger = logging.getLogger(__name__)
