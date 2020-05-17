import os
from producer_server import ProducerServer

os.environ["kafka_bootstrap"] = "localhost:9092"


def run_kafka_server():

    input_file = "data/police-department-calls-for-service.json"
    producer = ProducerServer(input_file = input_file,
                              topic="police_calls")
    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()

if __name__ == "__main__":
    feed()
