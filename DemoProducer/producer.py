import os
import inspect
import time
import json
from configparser import ConfigParser
from DemoProducer.Factory import Factory
from kafka import KafkaProducer

import logging

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.INFO
)

log = logging.getLogger(__name__)

cur_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
par_dir = os.path.dirname(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))


def config(filename='producer.cfg', section='kafka-producer'):
    # create a parser
    parser = ConfigParser()
    # read config file
    config_file = os.path.join(par_dir, "config", filename)
    print(config_file)
    parser.read(config_file)

    # get section, default to postgresql
    p_config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            # print("param 0: ", param[0], param[1])
            p_config[param[0]] = param[1]
    else:
        raise Exception('Section {} not found in the {} file'.format(section, filename))
    ssl_file_path = par_dir
    for opt in ("ssl_cafile", "ssl_certfile", "ssl_keyfile"):
        p_config[opt] = os.path.join(par_dir, "DemoProducer", p_config[opt])
        print(p_config[opt])
    return p_config


class DemoProducer(object):

    def __init__(self, topic_name, **configs):

        self.topic = topic_name
        if 'value_serializer' not in configs:
            configs['value_serializer'] = lambda v: json.dumps(v).encode('utf-8')
        configs['acks'] = 0
        configs['retries'] = 0
        if 'key_serializer' not in configs:
            configs['key_serializer'] = str.encode
        print(configs)
        self.producer = KafkaProducer(**configs)
        print("Created Kafka Producer")

    def main(self):
        t = Factory(self.producer, self.topic)
        t.start()
        time.sleep(3)
        t.producer_stop.set()
        t.join()


if __name__ == "__main__":
    kwargs = config()
    topic = kwargs.pop('topic')
    p = DemoProducer(topic, **kwargs)
    p.main()
