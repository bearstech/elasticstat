#!/usr/bin/env python

import socket
import json

import redis

stat = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#stat.connect(('localhost', 8125))


class EventsHose(object):
    def __init__(self, redis_connection, chan='packetbeat'):
        self.r = redis_connection
        self.chan = chan

    def __iter__(self):
        while True:
            chan, packet = r.blpop('packetbeat')
            packet = json.loads(packet)
            yield packet


class TrackBulkSize(object):
    def __init__(self, events):
        self.events = events

    def __iter__(self):
        for packet in self.events:
            if packet['http'] is None:
                    continue
            content_length = packet['http']['content_length']
            responsetime = packet['responsetime']
            agent = packet['agent']
            host = packet['http']['host']
            request_len = len(packet['request_raw'])
            response_len = len(packet['response_raw'])
            uri = packet['http']['request']['uri']
            s = uri.split('?')
            path = s[0]
            method = packet['http']['request']['method']
            src_ip = packet['src_ip']
            code = packet['http']['response']['code']
            ts = packet['@timestamp']
            if path == '/_bulk':
                #print json.dumps(packet, indent=2)

                #header, body = packet['request_raw'].split('\r\n\r\n', 1)
                #print (len(body.split('\n'))-1)/2

                header, body = packet['response_raw'].split('\r\n\r\n', 1)
                response = json.loads(body)
                errors = len([a for a in response['items'] if 'error' in a.values()[0]])
                idx = response['items'][0].values()[0]['_index']

                #print errors, len(response['items'])
                #print response
                yield dict(agent=agent, ts=ts, source=src_ip, code=code,
                           method=method, responsetime=responsetime,
                           request_len=request_len, response_len=response_len,
                           index=idx, bulk_size=len(response['items']),
                           bulk_errors=errors, uri=uri)

            #print agent, ts, src_ip, code, method, responsetime, 'ms', request_len, 'bytes', response_len, 'bytes', uri

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        host = sys.argv[1]
    else:
        host = 'localhost'
    r = redis.StrictRedis(host=host, port=6379, db=0)
    for event in TrackBulkSize(EventsHose(r)):
        print "{ts} {agent} {source} {code} {method} {responsetime} \
{request_len} {response_len} {index} {bulk_size} {bulk_errors} \
{uri}".format(**event)
