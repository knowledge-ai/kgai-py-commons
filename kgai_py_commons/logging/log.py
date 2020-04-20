"""
Generic log creator for the app/service
"""

import logging.config
import os
import pathlib
from logging import Logger

import yaml

from kgai_py_commons.util.singleton import Singleton


@Singleton
class TRLogger:

    def __init__(self):
        self._setup_logging()

    def _setup_logging(self):
        try:
            config_path = os.path.join(pathlib.Path(__file__).parent.absolute(), "log-config.yaml")
            with open(config_path, "r") as f:
                config = yaml.safe_load(f.read())
                logging.config.dictConfig(config)

            logger = self.get_logger(__name__)
            logger.debug("Logging configured..")

        except Exception as exp:
            print("Could not configure logging, error {}", exp)

    @staticmethod
    def get_logger(name: str) -> Logger:
        return logging.getLogger(name)
