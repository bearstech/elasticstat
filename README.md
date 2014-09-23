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

    [output.redis]
        enabled = true
        host = "somewher"
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

Futur
-----

 * More ready to use bug pattern.
 * Counting with statsd and graphite.
 * Plugging to Panda.
 * Some graph porn.


Licence
-------

3 Terms BSD Licence, © 2014 Mathieu Lecarme
