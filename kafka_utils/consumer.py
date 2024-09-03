from abc import abstractmethod

from kafka import KafkaConsumer
import signal
import json
import sys

from mylogger import get_logger

logger = get_logger()


class BaseConsumer:
    def __init__(self, kafka_host, kafka_port, kafka_group, kafka_topic):
        self.kafka_server = f"{kafka_host}:{kafka_port}"
        self.topic = kafka_topic
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_server,
            auto_offset_reset="earliest",  # Start reading at the earliest message
            enable_auto_commit=True,
            group_id=kafka_group,
            # group_id="crypto-group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )
        self.running = True
        self.name = self.__class__.__name__

        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown_consumer)

    def shutdown_consumer(self, signum, frame):
        logger.info(f"Shutting down {self.name}...")
        self.running = False
        self.consumer.close()
        sys.exit(0)

    def start(self):
        logger.info(f"Starting {self.name} for topic {self.topic}")
        for message in self.consumer:
            if not self.running:
                break
            self.process_message(message.value)

    @abstractmethod
    def process_message(self, message):
        """Method to consume messages sent by producer. Should be implemented in the child class."""
        pass
