from common.logging import prepare_logging
from common.kafka_producer import Producer
import base64
import logging
logger = prepare_logging(__name__, logging.INFO)
import json
import time

class ProducerServer():

    def __init__(self, input_file, topic, recreate=False, **kwargs):
        self.input_file = input_file
        self.topic = topic
        self.recreate = recreate
        self.p = Producer(topic_name=self.topic, recreate=self.recreate)


    # TODO we're generating a dummy data
    def generate_data(self):
        with open(self.input_file) as f:
            records = json.load(f)
            for record in records:
                try:
                    self.p.producer.produce(self.topic, self.dict_to_binary(record))
                    self.p.producer.poll(0)
                    logger.info("Pushed id: {}".format(record['crime_id']))
                except BufferError as e:
                    logger.info("Buffer full, waiting for free space on the queue")
                    self.producer.p.poll(10)

                #time.sleep(0.1)

    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict)

"""
import os
from producer_server import ProducerServer
os.environ["kafka_bootstrap"] = "localhost:9092"
file = "police-department-calls-for-service.json"
file_prod = ProducerServer(input_file = file, topic="police_calls")
file_prod.generate_data()
"""

