#!/usr/bin/env python
# encoding:utf8

"""
Ugly hack for handling packetbeat flow as pubsub.
"""

import json

import redis

def pop2publish(redis_connection, list_channel='packetbeat', publish_channel='/packetbeat/'):
    while True:
        chan, raw = redis_connection.blpop(list_channel)
        packet = json.loads(raw)
        print packet['agent']
        r.publish(publish_channel + packet['agent'], raw)


if __name__ == '__main__':

    import sys
    host = sys.argv[1]
    r = redis.StrictRedis(host=host, port=6379, db=0)
    pop2publish(r)
