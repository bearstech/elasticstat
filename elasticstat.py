#!/usr/bin/env python
# encoding:utf8

import socket
import json
import re

import redis


SLASHSLASH = re.compile('/+')


def split_slugs(uri):
    slugs = SLASHSLASH.split(uri)
    for a in [0, -1]:
        if not len(slugs[a]):
            del slugs[a]
    return slugs


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


def parse_headers(raw):
    d = {}
    for line in raw.split('\r\n')[1:]:
        k, v = line.split(': ', 1)
        d[k.lower()] = v
    return d


class HttpRequest(object):
    def __init__(self, http, raw):
        self.raw = raw
        self.host = http['host']
        self.uri = http['request']['uri']
        self.method = http['request']['method']
        s = self.uri.split('?')
        self.path = s[0]
        if len(s) > 1:
            self.arguments = s[1]
        else:
            self.arguments = None
        self.path = s[0]
        self.header, self.body = self.raw.split('\r\n\r\n', 1)
        self.header = parse_headers(self.header)

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
    def __init__(self, redis_connection, chan='/packetbeat/*'):
        self.r = redis_connection
        self.chan = chan

    def __iter__(self):
        pubsub = self.r.pubsub()
        pubsub.psubscribe(self.chan)
        while True:
            msg = pubsub.get_message()
            if msg is None or msg['type'] not in {'message', 'pmessage'}:
                continue
            packet = json.loads(msg['data'])
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
                   bulk_size=len(event.http.request.body.split('\n')),
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
            slugs = event.http.request.path.split('/')[1:]
            if slugs[-1] != '_search':
                continue
            yield event.timestamp, event.responsetime, slugs[:-1]


class TrackUsers(object):

    def __init__(self, events):
        self.events = events

    def __iter__(self):
        for event in self.events:
            if event.http is None:
                continue
            slugs = split_slugs(event.http.request.path)
            action = "?"
            if len(slugs) > 0:
                if slugs[-1][0] == '_':
                    action = slugs[-1][1:]
                elif slugs[0][0] == '_':
                    action = slugs[0][1:]
            yield event.timestamp, event.agent, event.responsetime, '%s:%i' % (event.raw['src_ip'], event.raw['src_port']), \
                '%s:%i' % (event.raw['dst_ip'], event.raw['dst_port']), \
                '[%s]' % event.http.request.header.get('user-agent', ''), \
                event.http.request.method, action, event.http.request.uri

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
        output = None
        last = None
        for ts, rt, slugs in TrackSlowSearch(EventsHose(r)):
            if last is None or last != ts[:10]:
                last = ts[:10]
                if output is not None:
                    output.close()
                output = open('slow-{ts}.csv'.format(ts=ts[:10]), 'a')
            line = "{ts};{rt};{slugs}".format(ts=ts, rt=rt,
                                              slugs=";".join(slugs))
            output.write(line)
            output.write('\n')
            print line
    if action == 'users':
        for a in TrackUsers(EventsHose(r)):
            print " ".join([str(b) for b in a])
