"""
Tests logging capabilities
"""
import pytest

from kgai_py_commons.logging.log import TRLogger


class TestLog(object):
    @classmethod
    def setup_class(cls):
        """
        setup common class functionalism
        """
        cls.logger = TRLogger.instance().get_logger(__name__)

    def test_logger_info(self, caplog):
        self.logger.info("Logging a info")
        assert "Logging a info" in caplog.text

    def test_singleton_exception(self):
        with pytest.raises(TypeError, match=r"Singletons must be *.*"):
            TRLogger()

    def test_singleton(self):
        assert self.logger == TRLogger.instance().get_logger(__name__)
