#!/usr/bin/env python
# encoding:utf8
import gevent.monkey
gevent.monkey.patch_all()

import socket
import json
import re
import time
import logging
import logging.handlers
from pprint import pprint

import yaml
import redis
from statsd import StatsClient
from raven import Client as Raven


from error import parseElasticsearchError


SLASHSLASH = re.compile('/+')
UNQUOTE = re.compile("'([^ \{\}\[\]<>\n]+?)'")

logger = logging.getLogger(__name__)

raven = Raven()

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


    @property
    def json(self):
        return json.loads(self.body)

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
    def __init__(self, redis_connection, chan='packetbeat/*'):
        self.r = redis_connection
        self.chan = chan
        assert self.r.ping()

    def __iter__(self):
        pubsub = self.r.pubsub()
        pubsub.psubscribe(self.chan)
        while True:
            msg = pubsub.get_message()
            if msg is None:
                time.sleep(0.1)
                continue
            if msg['type'] in {'message', 'pmessage'}:
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
            if action == "bulk":
                bulk_size = event.http.request.body.count('\n') + 1
            else:
                bulk_size = 0
            request_len = len(event.http.request)
            response_len = len(event.http.response)
            yield event.timestamp, event.agent, event.responsetime, '%s:%i' % (event.raw['src_ip'], event.raw['src_port']), \
                '%s:%i' % (event.raw['dst_ip'], event.raw['dst_port']), \
                '[%s]' % event.http.request.header.get('user-agent', ''), \
                event.http.request.method, action, event.http.request.uri, \
                bulk_size, request_len, response_len


class TrackErrors(object):
    def __init__(self, events):
        self.events = events

    def __iter__(self):
        for event in self.events:
            if event.http is not None and event.http.response.code >= 400:
                rq = event.http.request
                yield (dict(method=rq.method,
                            url="http://%s%s" % (rq.host, rq.path),
                            query_string=rq.arguments,
                            headers=rq.header),
                       event.http.request.json, #[FIXME] handling mjson bulk
                       event.http.response.json,
                       event.responsetime,
                       event.agent,
                       event.raw['src_ip'],
                       event.http.request.body
                       )


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
    if len(args):
        chan = args.pop()
    else:
        chan = 'packetbeat/*'
    print("host", host, chan)
    r = redis.StrictRedis(host=host, port=6379, db=0)
    if action == 'bulksize':
        for event in TrackBulkSize(EventsHose(r, chan)):
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
        for ts, rt, slugs in TrackSlowSearch(EventsHose(r, chan)):
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
        logger.setLevel(logging.INFO)
        handler = logging.handlers.TimedRotatingFileHandler('users.log', when='D', interval=1)
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)
        statsd = StatsClient('localhost', 8125)
        for a in TrackUsers(EventsHose(r, chan)):
            t = a[2]
            action = a[7]
            statsd.timing('action.%s' % action, int(t))
            logger.info(" ".join([str(b) for b in a]))
    if action == 'errors':
        log = logging.getLogger('raven')
        log.setLevel(logging.DEBUG)
        handler = logging.handlers.TimedRotatingFileHandler('raven.log', when='D', interval=1)
        handler.setLevel(logging.DEBUG)
        log.addHandler(handler)
        hose = EventsHose(r, chan)
        dumper = yaml.Dumper
        for rq, query, message, ts, agent, source, body in TrackErrors(hose):
            status = message['status']
            print status
            request = UNQUOTE.subn(r"\1", yaml.dump(query, allow_unicode=True,
                                                    default_flow_style=False).replace('!!python/unicode ', ''))[0]
            print request
            rq['data'] = dict(body=body)
            error = parseElasticsearchError(message['error'])
            exceptions = set((error['name'],))
            indices = set()
            last = None
            for i, s in error['exceptions'].items():
                indices.add(i.split('][')[1])
                last = s[-1].keys()[0]
                for ex in s:
                    exceptions.add(ex.keys()[0])
            pprint(error)

            exception = {'values': [{'type': error['name'],
                                      'value': error['description']
                                      }]}

            raven.http_context(rq)
            raven.tags_context(dict( agent=agent,
                                    lastException=last
                                    ))
            raven.extra_context(dict(request=request,
                                     stacktrace=error['exceptions'],
                                     indices=list(indices),
                                     source=source,
                                     description=error['description']))
            print raven.capture('raven.events.Exception', time_spent=ts,
                                message="%s:%s" % (error['description'], last),
                                culprit=error['name'],
                                data=dict(exception=exception)
                                )

