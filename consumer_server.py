from common.logging import prepare_logging
import logging
logger = prepare_logging(__name__, logging.INFO)
import os
from common.kafka_consumer import native_consumer
import asyncio

os.environ["kafka_bootstrap"] = "PLAINTEXT://localhost:9092"
os.environ["topic_name"] = "police_calls"
os.environ["groupID"] = "0"
os.environ["package_size"] = "1"
package_size = int(os.getenv('package_size'))

async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    consumer = native_consumer(topic_name, package_size=package_size)

    while True:
        message_lst = consumer.consume_value_by_list()
        if len(message_lst) > 0:
            for message in message_lst:
                logger.info("Message with value {}".format(message))

        await asyncio.sleep(0.01)

def main():
    try:
        topic_name = os.getenv('topic_name')
        logger.info("Prepare to consume data from topic {}".format(topic_name))
        asyncio.run(consume(topic_name))
    except KeyboardInterrupt as e:
        print("shutting down")

if __name__ == "__main__":
    main()

"""
set kafka_bootstrap=PLAINTEXT://localhost:9092
set topic_name=police_calls
set groupID=0
set package_size=1

python -m consumer_server
"""