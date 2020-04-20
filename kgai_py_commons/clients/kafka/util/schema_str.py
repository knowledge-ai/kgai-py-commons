"""
helper to create the Kafka schema string
required by AVRO producers and consumers
"""
import json
from dataclasses import is_dataclass
from typing import TypeVar

from dataclasses_avroschema.schema_generator import SchemaGenerator

from kgai_py_commons.logging.log import TRLogger

T = TypeVar("T")


class SchemaStr(object):
    def __init__(self):
        self.logger = TRLogger.instance().get_logger(__name__)

    def create_schema(self, namespace: str, data_class: T) -> str:
        if not is_dataclass(data_class):
            raise AttributeError("Only supports dataclass for conversion for now")
        else:
            try:
                avro_schema = SchemaGenerator(data_class).avro_schema_to_python()
                avro_schema.update({"namespace": namespace})
                # we need json not string
                return json.dumps(avro_schema)
            except Exception as err:
                self.logger.error("Error converting to AVRO schema from {}, error: {}".format(data_class, err))
                raise err
