from elasticsearch import Elasticsearch, helpers
from typing import Dict, List, Tuple
import datetime as dt

from config.models import Analysis, Sentiment


NEWS_INDEX = 'news'

def _initialize() -> Elasticsearch:
    es = Elasticsearch()

    # Create required indices
    if not es.indices.exists(NEWS_INDEX):
        print('Creating news index')
        es.indices.create(index='news', ignore=400)

    return es

def reset():
    es = Elasticsearch()
    es.indices.delete(NEWS_INDEX)


# Helpers
def delete_by_source(es: Elasticsearch, source: str):
    query = '{ "query": { "match": { "source": "' + source + '"} } }'
    es.delete_by_query(NEWS_INDEX, query)
    print(f'All {source} documents have been deleted')

def update_timestamps(es: Elasticsearch, target: str):

    if target == 'wsb':
        res = es.search(
            body={
                'query': {
                    'match': {'source': 'wsb'}
                },
                'from': 0,
                'size': 10000
            },
            index=NEWS_INDEX
        )
        if 'hits' in res and 'hits' in res['hits']:
            for obj in res['hits']['hits']:
                    obj_id = obj['_id']
                    timestamp = obj['_source']['timestamp']
                    es.update(
                        index=NEWS_INDEX,
                        id=obj_id,
                        body={
                            'doc': {
                                'timestamp': int(timestamp)
                            }
                        }
                    )
    elif target == 'seeking_alpha':
        res = es.search(
            body={
                'query': {
                    'match': {'source': 'seeking'}
                },
                'from': 0,
                'size': 10000
            },
            index=NEWS_INDEX
        )
        if 'hits' in res and 'hits' in res['hits']:
            for obj in res['hits']['hits']:
                    obj_id = obj['_id']
                    timestamp = obj['_source']['timestamp']
                    if type(timestamp) == int: continue
                    print(type(timestamp))

                    if type(timestamp) == str: new_timestamp = dt.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S%z').replace(tzinfo=dt.timezone.utc).timestamp()
                    elif type(timestamp) == float: new_timestamp = int(timestamp)

                    print('new timestamp:', new_timestamp)
                    es.update(
                        index=NEWS_INDEX,
                        id=obj_id,
                        body={
                            'doc': {
                                'timestamp': int(new_timestamp)
                            }
                        }
                    )

    print('all timestamps updated')

def update_sources(es: Elasticsearch, target_source: str):
    res = es.search(
        body={
            'query': {
                'match': {'source': target_source}
            },
            'from': 0,
            'size': 10000
        },
        index=NEWS_INDEX
    )
    print(f'updating {len(res["hits"]["hits"])} entries')
    if 'hits' in res and 'hits' in res['hits']:
        for obj in res['hits']['hits']:
                obj_id = obj['_id']
                old_source: str = obj['_source']['source']
                es.update(
                    index=NEWS_INDEX,
                    id=obj_id,
                    body={
                        'doc': {
                            'source': old_source.replace(' ', '_')
                        }
                    }
                )

def reindex(es: Elasticsearch, from_index: str, target_index: str, source: str):
    # see: https://stackoverflow.com/questions/25144034/how-to-copy-some-elasticsearch-data-to-a-new-index/38850817
    # TODO: create index if it doesn't yet exist    
    es.reindex(body={
        "source": {
            "index": from_index,
            "query": {
                "term": {
                    "source": source
                }
            }
        },
        "dest": {
            "index": target_index
        }
    })


# Saving
def add_analyses(analyses: List[Analysis], reset_indices: bool = False, bulk: bool = False):
    if reset_indices: reset()
    es = _initialize()

    if bulk:
        for analysis in analyses:
            actions = [
                {
                    '_index': NEWS_INDEX,
                    '_type': '_doc',
                    '_source': entry.__dict__
                } for entry in analysis.data
            ]
            helpers.bulk(es, actions)
    else:
        for analysis in analyses:
            for entry in analysis.data:
                es.index(NEWS_INDEX, entry.__dict__)





# News fetching
available_sources = ['seeking_alpha', 'cnbc', 'ap', 'benzinga', 'investors', 'bloomberg', 'reuters', 'market_watch', 'wsb', 'hacker_news']
def fetch_impressions(symbol: str, from_time: int, to_time: int, sources: List[str] = available_sources) -> Tuple[List[Sentiment], Dict[str, int]]:
    es = _initialize()

    entries: List[Sentiment] = []
    source_counts: Dict[str, int] = {}
    limit = 10000
    page = es.search(
        body={
            'query': {
                'bool': {
                    'must': [
                        { 'match': { 'symbol': symbol }} ,
                        {  'terms': { 'source': sources  }}
                    ],
                    'filter': [
                        { 'range': { 'timestamp': { 'gte': from_time }}},
                        { 'range': { 'timestamp': { 'lt': to_time }}}
                    ]
                }
            },
            'sort': [{ 'timestamp' : 'asc' }],
            'aggs': {
                'n_sources' : { 'cardinality' : { 'field' : 'source' } },
                'sources' : { 'terms' : { 'field' : 'source',  'size' : 500 } },
            },
            'size': limit
        },
        index=NEWS_INDEX,
        doc_type='_doc',
        scroll = '2m',
    )
    for hit in page['hits']['hits']: entries.append(Sentiment(**(hit['_source'])))
    for bucket in page['aggregations']['sources']['buckets']: source_counts[bucket['key']] = bucket['doc_count']

    # Scroll through additional results if needed
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']['value']
    while (scroll_size > 0):
        page = es.scroll(scroll_id = sid, scroll = '2m')
        sid = page['_scroll_id']
        scroll_size = len(page['hits']['hits'])
        for hit in page['hits']['hits']: entries.append(Sentiment(**(hit['_source'])))

    return entries, source_counts







# Debug
def update_mappings(es: Elasticsearch):
    print('Updating mappings')
    # es = _initialize()
    # es.indices.put_mapping(
    #     body={
    #         'properties': {
    #             'source': {
    #                 'type': 'keyword'
    #                 # 'fielddata': True
    #             }
    #         }
    #     },
    #     index=NEWS_INDEX,
    #     doc_type='_doc'
    # )

    # Set symbol as field data
    # es.indices.put_mapping(
    #     body={
    #         'properties': {
    #             'symbol': {
    #                 'type': 'text',
    #                 'fielddata': True
    #             }
    #         }
    #     },
    #     index=NEWS_INDEX,
    #     # doc_type='_doc'
    # )

    # Set timestamp date format
    es.indices.put_mapping(
        body={
            'properties': {
                'timestamp': {
                    'type': 'date',
                    'format': 'strict_epoch_second'
                }
            }
        },
        index=NEWS_INDEX
    )
    mappings = es.indices.get_field_mapping('source,timestamp,rating,symbol')

    print(mappings)

def data_stats():
    es = _initialize()
    res = es.search(
            body={
                'query': {
                    'bool': {
                        'must': [
                            {  'term': { 'source': 'wsb'  }}
                        ]
                    }
                },
                'aggs': {
                    'tag_cardinality' : { 'cardinality' : { 'field' : 'source' } },
                    'n_sources' : { 'terms' : { 'field' : 'source',  'size' : 500 } },
                    'max_date' : { "max" : { "field" : "timestamp" } },
                    'min_date' : { "min" : { "field" : "timestamp" } }
                },
                'size': 10
            }
        )

    print(res)

