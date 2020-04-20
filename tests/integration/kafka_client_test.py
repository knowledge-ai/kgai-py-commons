"""
Integration tests for the Kafka client
"""
import pytest

from kgai_py_commons.clients.kafka.consumer.consumer import TRAvroConsumer
from kgai_py_commons.clients.kafka.producer.producer import TRAvroProducer
from kgai_py_commons.model.googlenews.news_article import NewsArticle
from kgai_py_commons.model.googlenews.source_article import SourceArticle


class TestKafkaClient:
    @classmethod
    def setup_class(cls):
        cls.kafka_avro_producer = TRAvroProducer(schema_registry="http://localhost:8081",
                                                 bootstrap_servers="localhost:9092",
                                                 namespace="tests.py.commons", data_class=NewsArticle)
        cls.kafka_json_producer = TRAvroProducer(bootstrap_servers="localhost:9092")
        cls.kafka_avro_consumer = TRAvroConsumer(schema_registry="http://localhost:8081",
                                                 bootstrap_servers="localhost:9092",
                                                 namespace="tests.py.commons", group_id="tests.py.commons.consumer01",
                                                 data_class=NewsArticle)
        cls.kafka_json_consumer = TRAvroConsumer(bootstrap_servers="localhost:9092",
                                                 group_id="tests.py.commons.consumer01",
                                                 data_class=NewsArticle)
        cls.news_article = NewsArticle(title="Integration tests", publishedAt="2/02/2020", url="localhost:80",
                                       urlToImage=None, content="some content of the integration test", author=None,
                                       description=None,
                                       source=SourceArticle(name="integration tests", id="test-01",
                                                            description="integration tests", category=None,
                                                            language=None, url=None, country=None))

    @pytest.mark.skip(reason="delivery report not working, check later")
    def test_avro_producer(self, caplog):
        self.kafka_avro_producer.produce(topic="test-topic-avro", key="1234-avro", value=self.news_article)
        assert "Message delivered to test-topic" in caplog.text

    def test_json_producer(self, caplog):
        self.kafka_json_producer.produce(topic="test-topic-json", key="1234-json", value=self.news_article)
        assert "Message delivered to test-topic" in caplog.text

    @pytest.mark.skip(reason="figure out a way to interrupt the function and get results back")
    def test_json_consumer(self):
        self.kafka_json_consumer.consume(topic="test-topic-json")

    @pytest.mark.skip(reason="figure out a way to interrupt the function and get results back")
    def test_avro_consumer(self):
        self.kafka_avro_consumer.consume(topic="test-topic-avro")
