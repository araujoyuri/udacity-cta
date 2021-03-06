"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer


KAFKA_SERVER = 'localhost:9092'
SCHEMA_REGISTRY = 'http://localhost:8081'

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            'schema.registry.url': SCHEMA_REGISTRY,
            'bootstrap.servers': KAFKA_SERVER,
            'group.id': 'groupid'
        }

        self.admin_client = AdminClient({'bootstrap.servers': KAFKA_SERVER})

        topic_metadata = self.admin_client.list_topics()

        # If the topic does not already exist, try to create it
        if not topic_metadata.topics.get(self.topic_name):
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(config=self.broker_properties, default_key_schema=self.key_schema, default_value_schema=self.value_schema)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        futures = self.admin_client.create_topics([NewTopic(self.topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas)])

        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic {topic} created")

            except Exception as e:
                logger.error(f"Failed to create topic {topic} with error {e}")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        logger.info("Flush producer")
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
