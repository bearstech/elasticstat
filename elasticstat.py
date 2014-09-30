#!/usr/bin/env python
# encoding:utf8

import socket
import json

import redis


class Statsite(object):
    "Statsite client, it's just statd with TCP connection"
    def __init__(self, host='localhost', port=8125):
        self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.conn.connect(('localhost', 8125))


class Event(object):
    def __init__(self, raw):
        self.raw = raw
        self.timestamp = raw['@timestamp']
        self.responsetime = raw['responsetime']
        self.src_ip = raw['src_ip']
        self.agent = raw['agent']

    @property
    def http(self):
        if self.raw['http'] is None:
            return None
        return Http(self.raw)


class Http(object):
    def __init__(self, raw):
        self.raw = raw
        self.request = HttpRequest(self.raw['http'], self.raw['request_raw'])
        self.response = HttpResponse(self.raw['http'], self.raw['response_raw'])


class HttpRequest(object):
    def __init__(self, http, raw):
        self.raw = raw
        self.host = http['host']
        self.uri = http['request']['uri']
        self.method = http['request']['method']
        s = self.uri.split('?')
        if len(s) > 1:
            self.arguments = s[1]
        else:
            self.arguments = None
        self.path = s[0]

    def __len__(self):
        return len(self.raw)


class HttpResponse(object):
    def __init__(self, http, raw):
        self.raw = raw
        self.code = http['response']['code']
        self._header = None
        self._body = None
        self._json = None

    def _parse(self):
        self._header, self._body = self.raw.split('\r\n\r\n', 1)

    def __len__(self):
        return len(self.raw)

    @property
    def body(self):
        if self._body is None:
            self._parse()
        return self._body

    @property
    def header(self):
        if self._header is None:
            self._parse()
        return self._header

    @property
    def json(self):
        if self._json is None:
            self._json = json.loads(self.body)
        return self._json


class EventsHose(object):
    "Source of events"
    def __init__(self, redis_connection, chan='packetbeat'):
        self.r = redis_connection
        self.chan = chan

    def __iter__(self):
        while True:
            chan, packet = r.blpop('packetbeat')
            packet = json.loads(packet)
            yield Event(packet)


class Filter(object):
    def __init__(self, events):
        self.events = events


class BulkFilter(Filter):
    def bulk(self, event):
        raise NotImplementedError()

    def __iter__(self):
        for event in self.events:
            if event.http is None:
                continue
            if event.http.request.path == '/_bulk':
                e = self.bulk(event)
                if e is not None:
                    for b in e:
                        yield b


class TrackBulkSize(BulkFilter):
    "Iterator for tracking bulks, their sizes, their errors."

    def bulk(self, event):
        errors = len([a for a in event.http.response.json['items']
                      if 'error' in a.values()[0]])
        idx = event.http.response.json['items'][0].values()[0]['_index']

        yield dict(agent=event.agent, ts=event.timestamp,
                   source=event.src_ip, code=event.http.response.code,
                   method=event.http.request.method,
                   responsetime=event.responsetime, index=idx,
                   request_len=len(event.http.request),
                   response_len=len(event.http.response),
                   bulk_size=len(event.http.response.json['items']),
                   bulk_errors=errors, uri=event.http.request.uri)


class TrackBulkError(BulkFilter):

    def bulk(self, event):
        for item in event.http.response.json['items']:
            action, info = item.items()[0]
            if 'error' in info:
                yield event, info


class TrackSlowSearch(object):

    def __init__(self, events, *index):
        self.events = events
        self.index = index

    def __iter__(self):
        for event in self.events:
            slugs = event.http.request.path.split('/')
            if slugs[-1] != '_search':
                continue
            yield event.responsetime, slugs


if __name__ == '__main__':
    import sys
    args = sys.argv
    args.reverse()
    args.pop()
    if len(args):
        action = args.pop()
    else:
        action = 'bulksize'
    if len(args):
        host = args.pop()
    else:
        host = 'localhost'
    r = redis.StrictRedis(host=host, port=6379, db=0)
    if action == 'bulksize':
        for event in TrackBulkSize(EventsHose(r)):
            print "{agent} {ts} {source} {responsetime} ms \
⬆︎ {request_len} bytes ⬇︎ {response_len} bytes {index} {bulk_size} \
{bulk_errors}☠ [{code} {method} {uri}]".format(**event)
    if action == 'bulkerrors':
        for event, error in TrackBulkError(EventsHose(r)):
            print "{agent} {ts} {source} ".format(agent=event.agent,
                                                  ts=event.timestamp,
                                                  source=event.src_ip),
            print "{status} {_index} {_type} {_id} : {error}".format(**error)
    if action == 'slowsearch':
        for event in TrackSlowSearch(EventsHose(r)):
            print event
