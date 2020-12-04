#!/usr/bin/python3
import sys
import os.path
import argparse
from DemoProducer import producer
from DemoConsumer import consumer
import logging

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.DEBUG
)

log = logging.getLogger(__name__)

# append site package path (virtual env)
#sys.path.append('./lib/python3.8/site-packages/')
print(sys.path)


def main():
    parser = argparse.ArgumentParser()

    # command line arguments
    parser.add_argument(
        "--bootstrap-servers",
        help="Bootstrap server with hostname:port",
        nargs="+",
        required=True
    )
    parser.add_argument(
        "--security-protocol",
        help="Absolute path to CA certificate (*.pem)",
        default="PLAINTEXT"
    )
    parser.add_argument(
        "--ssl-cafile",
        help="Absolute path to CA certificate (*.pem)",
    )
    parser.add_argument(
        "--ssl-certfile",
        help="Absolute path Kafka certificate key (*.cert)",
    )
    parser.add_argument(
        "--ssl-keyfile",
        help="Absolute path to Kafka access key file (*.key)",
    )
    parser.add_argument(
        "--consumer",
        default=False,
        help="Run Kafka DemoConsumer",
        action='store_true'
    )
    parser.add_argument(
        "--producer",
        default=False,
        help="Run Kafka DemoProducer",
        action='store_true'
    )
    parser.add_argument(
        "--topic",
        help="Topic name",
        required=True
    )

    argvs = parser.parse_args()
    arg_consumer = argvs.consumer
    arg_producer = argvs.producer
    arg_topic = argvs.topic

    if not consumer and not producer:
        print("Please select consumer or producer")

    if argvs.ssl_cafile:
        validate_path(argvs)

    kwargs = {}
    for k, v in vars(argvs).items():
        if k not in ("topic", "producer", "consumer"):
            kwargs[k] = v

    print(kwargs)
    print(arg_producer)
    if arg_producer:
        p = producer.DemoProducer(arg_topic, **kwargs)
        p.main()
    elif arg_consumer:
        c = consumer.DemoConsumer(arg_topic, **kwargs)
        c.main()


def validate_path(argvs):
    for opt in ("ssl_cafile", "ssl_certfile", "ssl_keyfile"):
        path_ = getattr(argvs, opt)
        if not os.path.isfile(path_):
            print("File path {} of {} does not exists".format(path_, opt))


if __name__ == '__main__':
    main()
