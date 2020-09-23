from elasticsearch import Elasticsearch


client = Elasticsearch([{'host': 'localhost', 'port': 9200}])


def create_tradingview_index(index_name):
    # e.g. tradingview_ethusdt
    if client.indices.exists(index=index_name):
        print('Index {} already exists.'.format(index_name))
        return
    request_body = {
        "mappings": {
            "properties": {
                "ts": {
                    "type": "float",
                },
                "date": {
                    "type": "date",
                    "format": "strict_date_optional_time_nanos"
                },
                "author": {
                    "type": "keyword",
                },
                "title": {
                    "type": "text",
                },
                "body": {
                    "type": "text"
                },
                "url": {
                    "type": "keyword"
                },
                "move": {
                    "type": "keyword"
                }
            }
        },
        "settings" : {
            "index" : {
                "sort.field" : "date",
                "sort.order" : "desc"
            }
        }
    }
    client.indices.create(index=index_name, body=request_body)
    print('Finished creating index: {}'.format(index_name))


def main():
    for symbol in ['btcusdt', 'ethusdt']:
        create_tradingview_index(index_name='tradingview_{}'.format(symbol))


if __name__ == '__main__':
    main()
