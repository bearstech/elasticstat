import re

# shardFailures \{(?P<details>.*?)\}\]
SearchPhaseExecutionException = re.compile("(?P<blah>.*?); shardFailures (?P<details>.*)\]", re.MULTILINE)
ShardDetails = re.compile("\{(?P<shard>\[.+?\]\[.+?\]\[.+?\]): (?P<detail>.*?); \}", re.MULTILINE)
boring_exceptions = ('ElasticsearchException',
                     'UncheckedExecutionException')


def parseElasticsearchError(raw):
    name, blob = raw.split('[', 1)
    r = dict(name=name, description='', exceptions={})
    if name == "SearchPhaseExecutionException":
        m = SearchPhaseExecutionException.match(blob)
        r['description'] = m.group('blah')
        details =  m.group('details')
        for s in ShardDetails.finditer(details):
            k = s.group('shard')
            r['exceptions'][k] = []
            for n in s.group('detail').split('; nested: '):
                nname, ndetail = n.split('[', 1)
                if nname not in boring_exceptions:
                    r['exceptions'][k].append((nname, ndetail[:-1]))
    else:
        r['detail'] = blob[:-1]
    return r


if __name__ == '__main__':
    from pprint import pprint
    raw = """\
SearchPhaseExecutionException[Failed to execute phase [query], \
all shards failed; shardFailures \
{[FXRGwKYMT4uMBr0RSvgyfw][logstash-2014.11.21][0]: \
QueryPhaseExecutionException[[logstash-2014.11.21][0]: \
query[ConstantScore(*:*)],from[0],size[0]: Query Failed [Failed to execute global facets]]; \
nested: ElasticsearchException[org.elasticsearch.common.breaker.CircuitBreakingException: \
Data too large, data for field [@timestamp] would be larger than limit of [1911816192/1.7gb]]; \
nested: UncheckedExecutionException[org.elasticsearch.common.breaker.CircuitBreakingException: \
Data too large, data for field [@timestamp] would be larger than limit of [1911816192/1.7gb]]; \
nested: CircuitBreakingException[Data too large, data for field [@timestamp] \
would be larger than limit of [1911816192/1.7gb]]; }\
{[y4LSHUbuSzyhLGjaUAk7-A][logstash-2014.11.21][1]: \
RemoteTransportException[[Bouba][inet[/blablah:9300]][search/phase/query]]; \
nested: QueryPhaseExecutionException[[logstash-2014.11.21][1]: \
query[ConstantScore(*:*)],from[0],size[0]: Query Failed [Failed to execute global facets]]; \
nested: ElasticsearchException[org.elasticsearch.common.breaker.CircuitBreakingException: \
Data too large, data for field [@timestamp] would be larger than limit of [1911816192/1.7gb]]; \
nested: UncheckedExecutionException[org.elasticsearch.common.breaker.CircuitBreakingException: \
Data too large, data for field [@timestamp] would be larger than limit of [1911816192/1.7gb]]; \
nested: CircuitBreakingException[Data too large, data for field [@timestamp] would be larger than limit of [1911816192/1.7gb]]; }\
{[y4LSHUbuSzyhLGjaUAk7-A][logstash-2014.11.21][2]: RemoteTransportException[[Bouba][inet[/blablah:9300]][search/phase/query]]; \
nested: QueryPhaseExecutionException[[logstash-2014.11.21][2]: query[ConstantScore(*:*)],from[0],size[0]: Query Failed [Failed to execute global facets]]; \
nested: ElasticsearchException[org.elasticsearch.common.breaker.CircuitBreakingException: Data too large, data for field [@timestamp] would be larger than limit of [1911816192/1.7gb]]; \
nested: UncheckedExecutionException[org.elasticsearch.common.breaker.CircuitBreakingException: Data too large, data for field [@timestamp] would be larger than limit of [1911816192/1.7gb]]; \
nested: CircuitBreakingException[Data too large, data for field [@timestamp] would be larger than limit of [1911816192/1.7gb]]; }{[FXRGwKYMT4uMBr0RSvgyfw][logstash-2014.11.21][3]: QueryPhaseExecutionException[[logstash-2014.11.21][3]: query[ConstantScore(*:*)],from[0],size[0]: Query Failed [Failed to execute global facets]]; \
nested: ElasticsearchException[org.elasticsearch.common.breaker.CircuitBreakingException: Data too large, data for field [@timestamp] would be larger than limit of [1911816192/1.7gb]]; \
nested: UncheckedExecutionException[org.elasticsearch.common.breaker.CircuitBreakingException: Data too large, data for field [@timestamp] would be larger than limit of [1911816192/1.7gb]]; \
nested: CircuitBreakingException[Data too large, data for field [@timestamp] would be larger than limit of [1911816192/1.7gb]]; }{[FXRGwKYMT4uMBr0RSvgyfw][logstash-2014.11.21][4]: QueryPhaseExecutionException[[logstash-2014.11.21][4]: query[ConstantScore(*:*)],from[0],size[0]: Query Failed [Failed to execute global facets]]; \
nested: ElasticsearchException[org.elasticsearch.common.breaker.CircuitBreakingException: Data too large, data for field [@timestamp] would be larger than limit of [1911816192/1.7gb]]; \
nested: UncheckedExecutionException[org.elasticsearch.common.breaker.CircuitBreakingException: Data too large, data for field [@timestamp] would be larger than limit of [1911816192/1.7gb]]; \
nested: CircuitBreakingException[Data too large, data for field [@timestamp] would be larger than limit of [1911816192/1.7gb]]; }]"""
    pprint(parseElasticsearchError(raw))
    print
    raw = """\
SearchPhaseExecutionException[Failed to execute phase [query], all shards failed; \
shardFailures {[uq-lldZBQiOfVqzPGJ__3g][trac][0]: \
SearchParseException[[trac][0]: \
from[-1],size[-1]: Parse Failure [Failed to parse source [\
{"filter": {"term": {"user": "bob"}, "range": {"changetime": {"to": 1483548912000, "from": 1279089360000}}}, "query": {"query_string": {"query": "choux", "default_operator": "AND"}}, "facets": {"status": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "status"}}, "changetime": {"date_histogram": {"field": "changetime", "interval": "week"}, "facet_filter": {"range": {"changetime": {"to": 1483548912000, "from": 1279089360000}}}}, "_type": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "_type"}}, "component": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "component"}}, "domain": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "domain"}}, "priority": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "priority"}}, "user": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "user"}}, "keywords": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "keywords"}}, "path": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "path"}}}, "highlight": {"pre_tags": ["<b>"], "fields": {"_all": {}, "body": {}, "description": {}, "comment.comment": {}, "summary": {}, "name": {}}, "post_tags": ["</b>"]}}]]]; \
nested: ElasticsearchParseException[Expected field name but got START_OBJECT "range"]; }\
{[uq-lldZBQiOfVqzPGJ__3g][trac][1]: SearchParseException[[trac][1]: from[-1],size[-1]: \
Parse Failure [Failed to parse source [{"filter": {"term": {"user": "bob"}, "range": {"changetime": {"to": 1483548912000, "from": 1279089360000}}}, "query": {"query_string": {"query": "choux", "default_operator": "AND"}}, "facets": {"status": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "status"}}, "changetime": {"date_histogram": {"field": "changetime", "interval": "week"}, "facet_filter": {"range": {"changetime": {"to": 1483548912000, "from": 1279089360000}}}}, "_type": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "_type"}}, "component": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "component"}}, "domain": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "domain"}}, "priority": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "priority"}}, "user": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "user"}}, "keywords": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "keywords"}}, "path": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "path"}}}, "highlight": {"pre_tags": ["<b>"], "fields": {"_all": {}, "body": {}, "description": {}, "comment.comment": {}, "summary": {}, "name": {}}, "post_tags": ["</b>"]}}]]]; \
nested: ElasticsearchParseException[Expected field name but got START_OBJECT "range"]; }{[kuqVEnnDS8qDsUapB2Kl2A][trac][2]: RemoteTransportException[[plouk][inet[/10.20.125.178:9300]][search/phase/query]]; \
nested: SearchParseException[[trac][2]: from[-1],size[-1]: Parse Failure [Failed to parse source [{"filter": {"term": {"user": "bob"}, "range": {"changetime": {"to": 1483548912000, "from": 1279089360000}}}, "query": {"query_string": {"query": "choux", "default_operator": "AND"}}, "facets": {"status": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "status"}}, "changetime": {"date_histogram": {"field": "changetime", "interval": "week"}, "facet_filter": {"range": {"changetime": {"to": 1483548912000, "from": 1279089360000}}}}, "_type": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "_type"}}, "component": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "component"}}, "domain": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "domain"}}, "priority": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "priority"}}, "user": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "user"}}, "keywords": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "keywords"}}, "path": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "path"}}}, "highlight": {"pre_tags": ["<b>"], "fields": {"_all": {}, "body": {}, "description": {}, "comment.comment": {}, "summary": {}, "name": {}}, "post_tags": ["</b>"]}}]]]; \
nested: ElasticsearchParseException[Expected field name but got START_OBJECT "range"]; }{[kuqVEnnDS8qDsUapB2Kl2A][trac][3]: RemoteTransportException[[plouk][inet[/10.20.125.178:9300]][search/phase/query]]; \
nested: SearchParseException[[trac][3]: from[-1],size[-1]: Parse Failure [Failed to parse source [{"filter": {"term": {"user": "bob"}, "range": {"changetime": {"to": 1483548912000, "from": 1279089360000}}}, "query": {"query_string": {"query": "choux", "default_operator": "AND"}}, "facets": {"status": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "status"}}, "changetime": {"date_histogram": {"field": "changetime", "interval": "week"}, "facet_filter": {"range": {"changetime": {"to": 1483548912000, "from": 1279089360000}}}}, "_type": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "_type"}}, "component": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "component"}}, "domain": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "domain"}}, "priority": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "priority"}}, "user": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "user"}}, "keywords": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "keywords"}}, "path": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "path"}}}, "highlight": {"pre_tags": ["<b>"], "fields": {"_all": {}, "body": {}, "description": {}, "comment.comment": {}, "summary": {}, "name": {}}, "post_tags": ["</b>"]}}]]]; \
nested: ElasticsearchParseException[Expected field name but got START_OBJECT "range"]; }{[kuqVEnnDS8qDsUapB2Kl2A][trac][4]: RemoteTransportException[[plouk][inet[/10.20.125.178:9300]][search/phase/query]]; \
nested: SearchParseException[[trac][4]: from[-1],size[-1]: Parse Failure [Failed to parse source [{"filter": {"term": {"user": "bob"}, "range": {"changetime": {"to": 1483548912000, "from": 1279089360000}}}, "query": {"query_string": {"query": "choux", "default_operator": "AND"}}, "facets": {"status": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "status"}}, "changetime": {"date_histogram": {"field": "changetime", "interval": "week"}, "facet_filter": {"range": {"changetime": {"to": 1483548912000, "from": 1279089360000}}}}, "_type": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "_type"}}, "component": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "component"}}, "domain": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "domain"}}, "priority": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "priority"}}, "user": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "user"}}, "keywords": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "keywords"}}, "path": {"facet_filter": {"term": {"user": "bob"}}, "terms": {"field": "path"}}}, "highlight": {"pre_tags": ["<b>"], "fields": {"_all": {}, "body": {}, "description": {}, "comment.comment": {}, "summary": {}, "name": {}}, "post_tags": ["</b>"]}}]]]; \
nested: ElasticsearchParseException[Expected field name but got START_OBJECT "range"]; }]"""
    e = parseElasticsearchError(raw)
    pprint(e)
