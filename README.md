Elastic stats
=============

A toolbox for fixing stranges behaviors of Elasticsearch in real life,
with creative users.

Packetbeat
----------

For non intrusive spying, [packetbeat](http://packetbeat.com/) agent are used.
Elasticsearch talks JSON over HTTP, nothing too hard to steal.

The configuration is simple, you can remove most of options, and just keep
redis output and http listening on port 9200.

    [output]
        [output.elasticsearch]
            enabled = false
        [output.redis]
            enabled = true
            host = "somewhere"
            port = 6379
    [protocols]
        [protocols.http]
        ports = [9200]


Packetbeat agent is kind with your CPU.

Analyzing events
----------------

I can plug another Elasticsearch and its Kibana (and even a Logstash).
Drilling Elasticsearch behavior with another Elasticsearch looks like Marvel.
I want to analyze the content of the protocol, not just yet another HTTP flow.

Elastischsearch provides lots of metrics, it's another valuable source of information.

The Python way
--------------

The library provides an iterator flooding events. Just filter them and count.

Read the source. It's short, documented, with an example.

Pubsub messages, not queue
--------------------------

For now, packetbeat sends messages to a Redis queue (a LIST).
Logstash can handle both queue and pubsub, I made a
[patch for Packetbeat to PUBLISHing message](https://github.com/packetbeat/packetbeat/pull/70)
, not released yet.

@pop2publish.py@ is quick hack for BLPOPping a list, and PUBLISHing it.

With supervisor to managing it :

    [program:pop2publish]
    command=/usr/bin/python /opt/elasticstat/pop2publish.py REDIS_IP
    directory=/opt/elasticstat
    user=nobody
    autostart=true
    autorestart=true

Now, you can run multiple analysis tools, hacking, braking stuff, and still get long running analysis.

Future
------

 * More ready to use bug pattern.
 * Counting with statsd and graphite.
 * Plugging to Panda.
 * Some graph porn.


Licence
-------

3 Terms BSD Licence, © 2014 Mathieu Lecarme
