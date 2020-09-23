import requests
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
import json
import click
import re
from dateutil.tz import UTC
from datetime import datetime
import time


client = Elasticsearch([{'host': 'localhost', 'port': 9200}])


def parse_body(body):
    out = re.sub("[\n]+", " ", body.text).strip()
    return out


def parse_move(move):
    out = move.text.strip().lower()
    return out


def get_date_string(ts):
    dt_utc = datetime.fromtimestamp(ts).astimezone(UTC)
    date_string = dt_utc.strftime("%Y-%m-%dT%H:%M:%S.%f0Z")
    return date_string


def index_exists_check(index_name):
    if not client.indices.exists(index=index_name):
        error_msg = "Index {} does not exist. Make sure to run init_es.py to create all indices.".format(index_name)
        raise ValueError(error_msg)


def has_hits(index_name):
    query = {"size": 1}
    res = client.search(index=index_name, body=query)
    hits = res['hits']['hits']
    if len(hits) > 0:
        return True
    else:
        return False


def get_last_saved_post(index_name):
    if not has_hits(index_name):
        return None
    query = {
        "size": 1,
        "sort": [
            {
                "date": {
                    "order": "desc"
                }
            }
        ]
    }
    res = client.search(index=index_name, body=query)
    most_recent_hit = res['hits']['hits'][0]
    return most_recent_hit['_source']


def new_post_made(top_post, index_name):
    last_saved_post = get_last_saved_post(index_name)
    if last_saved_post is None:
        return True
    if top_post['ts'] > last_saved_post['ts']:
        return True
    return False


def fetch(symbol):
    num_pages = 1
    cards_class = "tv-feed__item js_cb_class tv-feed-layout__card-item"
    bodies_class = "tv-widget-idea__description-row tv-widget-idea__description-row--clamped js-widget-idea__popup"
    records = []
    for i in range(1, num_pages + 1):
        if i == 1:
            url = f"https://www.tradingview.com/ideas/search/{symbol}/?sort=recent"
        else:
            url = f"https://www.tradingview.com/ideas/search/{symbol}/page-{i}/?sort=recent"
        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'lxml')
        cards = soup.find_all('div', {'class': cards_class})
        bodies = soup.find_all('p', {'class': bodies_class})
        times = soup.find_all('span', {'class': 'tv-card-stats__time js-time-upd'})
        move_dir = soup.find_all('span', {'class': re.compile('tv-idea-label tv-widget-idea__label tv-idea-label--*')})
        for card, body, time, move in zip(cards, bodies, times, move_dir):
            data_card = json.loads(card['data-card'])
            ts = float(time['data-timestamp'])
            record = {
                'id': data_card['data']['id'],
                'post': {
                    'ts': ts,
                    'date': get_date_string(ts),
                    'author': data_card['author']['username'],
                    'title': data_card['data']['name'],
                    'body': parse_body(body),
                    'url': data_card['data']['published_url'],
                    'move': parse_move(move)
                }
            }
            records.append(record)
    return reversed(records)  # records[0] oldest


def scrape(symbol, index_name, timeout):
    scrape_cnt = 0
    while True:
        records = fetch(symbol)
        curr_time = datetime.now()
        num_posts_added = 0
        last_saved_post = get_last_saved_post(index_name)
        for record in records:
            if not last_saved_post or record['post']['ts'] > last_saved_post['ts']:
                _ = client.index(index=index_name, id=record['id'], body=record['post'])
                num_posts_added += 1
        log_msg = 'Scrape: {} Current time: {} Added: {} Index: {}'.format(
            scrape_cnt,
            curr_time,
            num_posts_added,
            index_name
        )
        print(log_msg)
        time.sleep(timeout)
        scrape_cnt += 1


@click.command()
@click.option('--symbol', type=str, default="ethusdt")
@click.option('--timeout', type=int, default=5)
def main(symbol, timeout):
    index_name = 'tradingview_{}'.format(symbol)
    index_exists_check(index_name)
    scrape(symbol, index_name, timeout)


if __name__ == "__main__":
    main()


# todo: create API to fetch (max) 1000 tradingview posts (on a symbol).
