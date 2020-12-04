import threading
from DemoProducer.data import get_registered_user
from DemoProducer.data import get_key
from kafka.errors import KafkaError

import logging
logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )

log = logging.getLogger(__name__)


class Factory(threading.Thread):
    daemon = True

    def __init__(self, producer, topic, *args, **kwargs):
        super(Factory, self).__init__(*args, **kwargs)

        self.producer = producer
        self.producer_stop = threading.Event()
        self.topic = topic
        self.sent = 0

    @staticmethod
    def on_send_success(record_metadata):
        log.info(record_metadata.topic)
        log.info(record_metadata.partition)
        log.info(record_metadata.offset)
        # self.producer_stop.set()

    @staticmethod
    def on_send_error(exception):
        log.error("Error on getting record metadata:", exc_info=exception)

    def run(self):
        while not self.producer_stop.is_set():

            # Get fake key and value from faker module
            key = get_key()
            data = get_registered_user()

            # Send asynchronously with callbacks

            future_metadata = self.producer.send(self.topic, key=key, value=data)\
                .add_callback(self.on_send_success)\
                .add_errback(self.on_send_error)

            # Block for synchronous sends
            try:
                result = future_metadata.get(timeout=40)
                # print(result)
                self.sent += 1
            except KafkaError as e:
                log.exception(e)
        print("Sending data")
        print("Number of msgs sent: ", self.sent)
        self.producer.flush()

