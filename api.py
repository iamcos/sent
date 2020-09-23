from flask import Flask
from flask import request
from elasticsearch import Elasticsearch
import json


app = Flask(__name__)


client = Elasticsearch([{'host': 'localhost', 'port': 9200}])


@app.after_request
def after_request(response):
    header = response.headers
    header['Access-Control-Allow-Origin'] = '*'
    return response


@app.route('/tradingview/<symbol>', methods=['GET'])
def fetch(symbol):
    limit = request.args.get('limit', default=100)
    index_name = 'tradingview_{}'.format(symbol)
    query = {
        "size": limit,
        "sort": [{"date": "desc"}]
    }
    res = client.search(index=index_name, body=query)
    hits = res['hits']['hits']
    posts = [hit['_source'] for hit in hits]
    return json.dumps(posts)


if __name__ == '__main__':
    app.run(host='localhost', port=8000, debug=False)