from abc import abstractmethod
import signal

from kafka import KafkaProducer
import json
import sys
import time
from mylogger import get_logger

logger = get_logger()


class BaseProducer:
    def __init__(self, kafka_host, kafka_port, kafka_topic, interval):
        self.kafka_server = f"{kafka_host}:{kafka_port}"
        self.topic = kafka_topic
        self.interval = interval

        self.producer = KafkaProducer(
            bootstrap_servers=[self.kafka_server],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')  # Serialize data to JSON
        )
        self.name = self.__class__.__name__
        # gracefully shutdown the producer in case it is interrupted
        signal.signal(signal.SIGINT, self.shutdown_producer)

    @abstractmethod
    def produce_messages(self):
        """Method to produce messages to Kafka. Should be implemented in the child class."""
        pass

    def shutdown_producer(self):
        """Gracefully shutdown the producer."""
        logger.info(f'Shutting down {self.name}...')
        self.producer.flush()
        self.producer.close()
        sys.exit(0)

    def start(self):
        """Start the message production process."""
        logger.info(f"Starting {self.name} for topic {self.topic}")
        self.produce_messages()

    def sleep(self):
        logger.info(f"{self.name} Sleeping for {self.interval} seconds.......")
        time.sleep(self.interval)
