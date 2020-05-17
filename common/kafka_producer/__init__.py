from common.logging import prepare_logging
import logging
logger = prepare_logging(__name__, logging.INFO)

import time
import os
import socket
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer as ConfluentProducer

class Producer():

    retention_bytes = 1610612736

    def __init__(
        self,
        topic_name,
        num_partitions = 1,
        replication_factor = 1,
        cleanup = 'delete',
        retention = retention_bytes,
        compression='gzip',
        delete = '1300',
        file_delete = '2000',
        recreate = True,
        avro_prod = False,

    ):
        """

        :param topic_name:
        :param num_partitions:
        :param replication_factor:
        :param cleanup:
        :param retention:
        :param compression:
        :param delete:
        :param file_delete:
        :param recreate:
        """
        self.topic_name = topic_name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.cleanup = cleanup
        self.retention = retention
        self.compression = compression
        self.delete = delete
        self.file_delete = file_delete
        self.recreate = recreate
        self.avro_prod = avro_prod
        self.kafka_bootstrap = os.getenv('kafka_bootstrap')
        self.cliet_id = socket.gethostname()
        self.client = AdminClient({"bootstrap.servers": self.kafka_bootstrap})


        self.broker_properties = {
            "bootstrap.servers": self.kafka_bootstrap,
            "client.id": self.cliet_id,
            "linger.ms": 1000,
            "compression.type": self.compression,
            "batch.num.messages": 1000000,
        }



        # If the topic does not already exist, try to create it
        if self.topic_exists(self.client, self.topic_name):
            logger.info("Topic {} already exists".format(self.topic_name))
            if self.recreate:
                self.delete_topic(self.client, [self.topic_name])
                time.sleep(5)
                logger.info("Topic {} will be create".format(self.topic_name))
                self.create_topic(self.client)
            else:
                logger.info("Topic {} already exists in Kafka Cluster".format(self.topic_name))


        else:
            logger.info("Topic {} will be create".format(self.topic_name))
            self.create_topic(self.client)

        if not self.avro_prod:
            self.producer = ConfluentProducer(self.broker_properties)
        else:
            raise NotImplemented

    def topic_exists(self, client, topic_name):
        """Checks if the given topic exists"""
        topic_metadata = client.list_topics(timeout=5)
        return topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))

    def delete_topic(self, client, topic):

        topics = client.delete_topics(topic, operation_timeout=30)
        for topic, future in topics.items():
            try:
                future.result()
                logger.info(f"Topic {topic} deleted")
            except Exception as e:
                logger.fatal(f"Failed to create topic {topic}: {e}")




    def create_topic(self, client):
        topics = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.replication_factor,
                    config={
                        "cleanup.policy": self.cleanup,
                        "retention.bytes": self.retention,
                        "compression.type": self.compression,
                        "delete.retention.ms": self.delete,
                        "file.delete.delay.ms": self.file_delete,
                    },
                )
            ]
        )

        for topic, future in topics.items():
            try:
                future.result()
                logger.info(f"Topic {topic} created")
            except Exception as e:
                logger.fatal(f"Failed to create topic {topic}: {e}")


    def close(self, delete=False):
        """Prepares the producer for exit by cleaning up the producer"""
        logger.debug("producer close function working")

        if self.producer is not None:
            if delete:
                self.delete_topic(self.client, [self.topic_name])

            logger.info("Flushing producer")
            self.producer.flush()



if __name__ == "__main__":
    os.environ["kafka_bootstrap"] = "kafka-dev-broadcast.C4CPET.c.eu-nl-1.cloud.sap:9094"
    p = Producer(topic_name='test')
    p.close(delete=True)





