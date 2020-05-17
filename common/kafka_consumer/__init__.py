from common.logging import prepare_logging
import logging
logger = prepare_logging(__name__, logging.INFO)

import os
from confluent_kafka import Consumer



class native_consumer():

    def __init__(self,
                 topic_name,
                 ssl=False,
                 package_size=10,
                 consume_freq=1
                 ):
        self.topic_name = topic_name
        self.package_size = package_size
        self.consume_freq = consume_freq
        self.broker = os.getenv('kafka_bootstrap')
        self.groupID = os.getenv('groupID')
        self.ssl = ssl

        if not self.ssl:
            self.native_consumer = Consumer({"bootstrap.servers": self.broker,
                                             "group.id": self.groupID})
        else:
            self.native_consumer = Consumer({"bootstrap.servers": self.broker,
                                             "security.protocol": "SSL",
                                             "ssl.ca.location": "ssl_certs/ca.crt",
                                             "group.id": self.groupID})

        self.native_consumer.subscribe([self.topic_name])

    def consume_value_by_list(self):
        try:
            messages = self.native_consumer.consume(self.package_size, self.consume_freq)
            message_lst = []
            for message in messages:
                message_lst.append(message.value().decode("UTF-8"))

            return message_lst
        except Exception as e:
            logger.error("Error while consuming topic with error {}".format(e))



