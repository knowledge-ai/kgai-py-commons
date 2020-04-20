"""
Helper for kafka consumer
"""
import json
import warnings
from dataclasses import is_dataclass
from typing import TypeVar, Type

from confluent_kafka import Consumer
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from dacite import from_dict

from kgai_py_commons.clients.kafka.util.schema_str import SchemaStr
from kgai_py_commons.logging.log import TRLogger

T = TypeVar("T")


class TRAvroConsumer(object):
    def __init__(self, bootstrap_servers: str, group_id: str, data_class: Type[T], schema_registry: str = None,
                 namespace: str = None):
        """
        constructs an AVRO consumer, to use AVRO specify the schema_registry, namespace params
        :param schema_registry:
        :param bootstrap_servers:
        :param namespace:
        :param group_id:
        :param data_class:
        """
        if not data_class:
            raise AttributeError("data_class cannot be null, it is required to map consumed data to class")
        if not is_dataclass(data_class):
            warnings.warn("only dataclasses are fully supported, others may cause unacceptable behaviour")

        self.logger = TRLogger.instance().get_logger(__name__)
        self.data_class = data_class
        self._is_avro = False
        if schema_registry:
            self._avro_producer(schema_registry, bootstrap_servers, namespace, group_id)
            self._is_avro = True
        else:
            self._json_producer(bootstrap_servers, group_id)

    def _avro_producer(self, schema_registry: str, bootstrap_servers: str, namespace: str, group_id: str):
        """
        onfigures a AVRO serializing producer to be used along with schema registry
        :param schema_registry:
        :param bootstrap_servers:
        :param namespace:
        :return:
        """
        # TODO: look for better support of AVRO serielization/deseriealization support
        # TODO: current method fails, check integration tests that are disabled
        warnings.warn("Experimental. Please do not use, needs adaptations")
        schema_registry_client = SchemaRegistryClient({"url": schema_registry})
        avro_deserializer = AvroDeserializer(SchemaStr().create_schema(data_class=self.data_class, namespace=namespace),
                                             schema_registry_client,
                                             self._from_dict_dataclass)
        consumer_conf = {"bootstrap.servers": bootstrap_servers,
                         "key.deserializer": StringDeserializer("utf_8"),
                         "value.deserializer": avro_deserializer,
                         "group.id": group_id,
                         "auto.offset.reset": "earliest"}
        self._consumer = DeserializingConsumer(consumer_conf)

    def _json_producer(self, bootstrap_servers: str, group_id: str):
        """
        creates a JSON serializing producer that does not use a schema registry.
        :param schema_registry:
        :param schema_str:
        :param bootstrap_servers:
        """
        consumer_conf = {"bootstrap.servers": bootstrap_servers, "group.id": group_id, "session.timeout.ms": 6000,
                         "auto.offset.reset": "earliest"}
        self._consumer = Consumer(consumer_conf)

    def _from_dict_dataclass(self, data_dict, ctx):
        """
        wraps a dacite mapper (from dict to data object) and makes it ready for confluent AVRO translators
        :param data_dict:
        :param ctx:
        :return:
        """
        if data_dict is None:
            return None
        return from_dict(data_class=self.data_class, data=data_dict)

    def _reciept_report(self, err: bool, msg: str, topic: str = None):
        """
        Called when a message is received or error occurs at the consumer
        :param err:
        :param msg:
        :param topic:
        :return:
        """
        if err is not None:
            self.logger.error("Consumer error: {}".format(msg))
        else:
            self.logger.info('Topic {}, Received message: {}'.format(topic, msg))

    def consume(self, topic: str):
        """
        TODO: move to asyncio
        start consumer
        :param topic:
        :return:
        """
        self._consumer.subscribe([topic])
        try:
            while True:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                elif msg.error():
                    self._reciept_report(err=True, msg=str(msg.error()))
                    continue
                elif not msg.value():  # empty body not acceptable, log and continue
                    self.logger.error("recieved empty message in topic: {}, key:{}".format(msg.topic(), msg.key()))
                else:
                    dataclass_obj = None
                    if not self._is_avro:
                        clean_msg = json.loads(msg.value().decode('utf-8'))
                        self._reciept_report(err=False, msg=clean_msg, topic=topic)
                        dataclass_obj = self._from_dict_dataclass(data_dict=clean_msg, ctx=None)
                    else:
                        dataclass_obj = msg.value()

        except KeyboardInterrupt:
            self.logger.warn("Interrupted by user/system, existing consumer....")
        finally:
            self._consumer.close()
