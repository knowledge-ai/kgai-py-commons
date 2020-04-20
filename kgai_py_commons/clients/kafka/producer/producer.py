import json
import warnings
from typing import TypeVar, Type

from confluent_kafka import SerializingProducer
from confluent_kafka.cimpl import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from kgai_py_commons.clients.kafka.util.schema_str import SchemaStr
from kgai_py_commons.logging.log import TRLogger

T = TypeVar("T")


class TRAvroProducer(object):
    def __init__(self, bootstrap_servers: str, schema_registry: str = None, namespace: str = None,
                 data_class: Type[T] = None):
        """
        configures the producer, to use AVRO specify the schema_registry, namespace and data_class params
        :param schema_registry:
        :param bootstrap_servers:
        :param namespace:
        :param data_class:
        """
        self.logger = TRLogger.instance().get_logger(__name__)
        self.data_class = data_class
        self._is_avro = False
        if schema_registry:
            self._avro_producer(schema_registry, bootstrap_servers, namespace)
            self._is_avro = True
        else:
            self._json_producer(bootstrap_servers)

    def _avro_producer(self, schema_registry: str, bootstrap_servers: str, namespace: str):
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
        avro_serializer = AvroSerializer(
            schema_str=SchemaStr().create_schema(data_class=self.data_class, namespace=namespace),
            schema_registry_client=schema_registry_client,
            to_dict=self.obj_to_dict)
        producer_conf = {"bootstrap.servers": bootstrap_servers,
                         "key.serializer": StringSerializer("utf_8"),
                         "value.serializer": avro_serializer}
        self._producer = SerializingProducer(producer_conf)

    def _json_producer(self, bootstrap_servers: str):
        """
        creates a JSON serializing producer that does not use a schema registry.
        :param schema_registry:
        :param schema_str:
        :param bootstrap_servers:
        """
        producer_conf = {"bootstrap.servers": bootstrap_servers}
        self._producer = Producer(producer_conf)

    def obj_to_dict(self, obj, ctx):
        """
        Method passed on to avro serializer to get dict from object
        needs extra param ctx
        :param cls:
        :param ctx:

        :return:
        """
        return json.loads(json.dumps(obj, default=lambda o: o.__dict__))

    def _delivery_report(self, err: str, msg: str):
        """
        Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush().
        :param err:
        :param msg:
        :return:
        """
        if err is not None:
            self.logger.error("Message delivery failed: {}".format(err))
        else:
            self.logger.info("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))

    def produce(self, topic: str, key: str, value: T):
        """
        produce data in the Kafka topic, key must be unique id
        :param topic:
        :param key:
        :param value:
        :return:
        """
        if self._is_avro:
            self._producer.produce(topic=topic, key=key, value=value,
                                   on_delivery=self._delivery_report)
        else:
            self._producer.produce(topic=topic, value=json.dumps(value, default=lambda o: o.__dict__), key=key,
                                   on_delivery=self._delivery_report)

        self._producer.flush()
