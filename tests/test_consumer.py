#!/usr/bin/python3

import pytest
import subprocess
import shlex
from _pytest.capture import capsys

from DemoConsumer.consumer import DemoConsumer as consumer
from kafka import errors


def error_cb(err):
    print("Error: ", err)


def execute(self, command, **kwargs):
    return subprocess.run(shlex.split(command),
                          stderr=subprocess.PIPE,
                          stdout=subprocess.PIPE,
                          universal_newlines=True,
                          **kwargs)


def test_basic(capsys):
    """Basic API tests"""

    with pytest.raises(TypeError) as e:
        c = consumer()

    assert e.match("__init__() missing 1 required positional argument: 'topic_name'")
    config = {'socket.timeout.ms': 10,
              'error_cb': error_cb,
              'message.timeout.ms': 10}
    topic = "test"
    c = consumer(topic, **config)

    assert "Created Kafka Consumer " == capsys.readouterr().out

    def on_delivery(err, msg):
        print('delivery', str)
        # Since there is no broker, produced messages should time out.
        assert err.code() == errors._MSG_TIMED_OUT

    c.main()
    assert "Number of msgs" in capsys.readouterr.out


def test_main(test_params=None):
    cmd = "./main.py " \
          "--bootstrap-servers {bootstrap-servers} " \
          "--ssl-cafile {ca_path} " \
          "--ssl-keyfile {access_key_path} " \
          "--ssl-certfile {access_cert_path} " \
        .format(**test_params)
    result = execute("{} --producer".format(cmd), check=True)



    assert "Created Kafka Producer" in result.stdout
    assert "Sending data" in result.stdout
