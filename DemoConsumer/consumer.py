import inspect
import json
import logging
import os
from configparser import ConfigParser

from kafka import KafkaConsumer
from kafka import TopicPartition

from Postgresql import db_connect

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.DEBUG
)

log = logging.getLogger(__name__)


class NonExistentTopic(Exception):
    pass


cur_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
par_dir = os.path.dirname(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))


def config(filename='consumer.cfg', section='kafka-consumer'):
    # create a parser
    parser = ConfigParser()
    # read config file
    config_file = os.path.join(par_dir, "config", filename)
    parser.read(config_file)

    # get section, default to postgresql
    c_config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            # print("param: ", param[0], param[1])
            c_config[param[0]] = param[1]
    else:
        raise Exception('Section {} not found in the {} file'.format(section, filename))

    sec_int = 'section-int'
    if parser.has_section(sec_int):
        params = parser.items(sec_int)
        for param in params:
            c_config[param[0]] = parser.getint(sec_int, param[0])
    # print(c_config)
    for opt in ("ssl_cafile", "ssl_certfile", "ssl_keyfile"):
        c_config[opt] = os.path.join(par_dir, "DemoConsumer", c_config[opt])
        print(c_config[opt])

    return c_config


class DemoConsumer(object):

    def __init__(self, topics_list, **kwargs):
        self.topics = topics_list
        self.config = kwargs
        if 'value_deserializer' not in self.config:
            self.config['value_deserializer'] = lambda v: json.loads(v.decode('utf-8'))
        self.consumer = KafkaConsumer(**self.config)
        self.consumer.subscribe(self.topics)
        print("Created Kafka Consumer")

        # manually assign list of TopicPartitions to the consumser
        # self.consumer.assign([TopicPartition('foo-bar', 2)])

    # Call poll twice. First call will just assign partitions for our
    # consumer without actually returning anything
    def poll(self):

        for _ in range(1):
            raw_msgs = self.consumer.poll(timeout_ms=1000)
            for tp, msgs in raw_msgs.items():
                for message in msgs:
                    try:
                        assert isinstance(message.value, dict)
                        print("{}:{}:{}: key={} value={}".format(message.topic, message.partition,
                                                                 message.offset, message.key,
                                                                 message.value))
                    except AssertionError:
                        print("Not a dictionary value")
                        log.error("Message value is corrupted")

        # Commit offsets so we won't get the same messages again
        # self.consumer.commit()

    def main(self):

        # self.check_non_existent_topic()
        collected_data = []
        count = 0

        for message in self.consumer:
            # message value and key are raw bytes -- decode if necessary!
            # e.g., for unicode: `message.value.decode('utf-8')`

            try:
                assert isinstance(message.value, dict)
                # print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                #                                      message.offset, str(message.key, 'utf-8'),
                #                                      message.value))
                _li = [(k, v) for k, v in message.value.items()]
                values = tuple([t[1] for t in _li])
                collected_data.append(values)
                count += 1
            except AssertionError:
                print("Not a dictionary value: {}".format(message.value))
                log.error("Message value is corrupted")
        print("Number of msgs", count)
        # print(collected_data[0], collected_data[1])
        db_connect.insert(collected_data)

        # self.consumer.commit()

    def check_non_existent_topic(self):

        for check_topic in self.topics:
            try:
                if check_topic not in self.consumer.topics():
                    raise NonExistentTopic
            except NonExistentTopic:
                print("'{}' topic does not exist in the host {}".
                      format(check_topic, self.consumer.config))
                log.error("'{}' topic does not exist in the host {}".
                          format(check_topic, self.consumer.config))


if __name__ == "__main__":
    kwargs = config()
    topics = kwargs.pop("topic")
    c = DemoConsumer(topics, **kwargs)

    c.main()
    # c.poll()
