import requests
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
import json
import click
import re
from dateutil.tz import UTC
from datetime import datetime
import time
from init_es import create_tradingview_index
import pandas as pd 
import logging
# logging.basicConfig(level=logging.DEBUG,
#                     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


client = Elasticsearch([{'host': 'localhost', 'port': 9200}])

def parse_description(id):
    pass


def parse_body(body):
    out = re.sub("[\n]+", " ", body.text).strip()
    print(f"\nStart: \n{out} \nEND\n")
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
    print(most_recent_hit['_source'])
    return most_recent_hit['_source']


def new_post_made(top_post, index_name):
    last_saved_post = get_last_saved_post(index_name)
    if last_saved_post is None:
        return True
    if top_post['ts'] > last_saved_post['ts']:
        return True
    return False


def additional_data():
    chart_description = driver.findElement(By.xpath("//div[@class='tv-chart-view__description selectable']"))
    
    bCHUSDTSym = driver.findElement(By.xpath('//div[@data-card={"data":{"id":{id}'))


def fetch(symbol,pages):
    num_pages = pages
    cards_class = "tv-feed__item js_cb_class tv-feed-layout__card-item"
    bodies_class = "tv-widget-idea__description-row tv-widget-idea__description-row--clamped js-widget-idea__popup"

    desc_class = "tv-chart-view__description"
    user_class = 'tv-chart-view__title-user-name tv-user-link__name'
  
    coin_class = "tv-chart-view__title-row--symbol"
    move_class = 'tv-widget-idea__label'
    time_class = 'tv-chart-view__title-time js-time-upd'
    likes_class = "tv-card-social-item__count"

    
    records = []
    for i in range(1, num_pages + 1):
      
        url = f"https://www.tradingview.com/symbols/{symbol}/ideas/page-{i}/?sort=recent"
        page = requests.get(url)
        main_page = BeautifulSoup(page.text, 'lxml')
        cards = main_page.find_all('div', {'class': cards_class})

        for card in cards:
            data_card = json.loads(card['data-card'])
            chart_url = data_card['data']['published_url']
            
            chart_page = requests.get(f'https://www.tradingview.com/chart/{chart_url}')
            soup = BeautifulSoup(chart_page.text, 'lxml')
            
            
            description = soup.find('div', attrs={'class': desc_class})
            coin = soup.find('div', attrs={'class': coin_class})
            move = soup.find('span', attrs={'class': move_class})
            time = soup.find('span', attrs={'class': time_class})
            likes = soup.find('span', attrs={'class': likes_class})
            user = soup.find('span', attrs={'class': user_class})
            
            try:
                description = description.text
                description = re.sub("[\n]+", " ", description).strip()
            except:
                pass
            try:
                coin = coin.text
                coin = re.sub("[\n]+", " ", coin).strip()
            except:
                pass
            try:
                move = move.text
                move = re.sub("[\n]+", " ", move).strip()
            except:
                pass
 
            try:
                likes = likes.text
                likes = re.sub("[\n]+", " ", likes).strip()
            except:
                pass
            try:
                user = user.text
                user = re.sub("[\n]+", " ", user).strip()
            except:
                pass
            ts = float(time['data-timestamp'])
            record = {
                'id': data_card['data']['id'],
                'post': {
                    'ts': ts,
                    'date': get_date_string(ts),
                    'author': user,
                    'title': None,
                    'body': description,
                    'url': f'https://www.tradingview.com/chart/{chart_url}',
                    'move': move
                }
            }
            # print(record[])
           
            if record not in records:
                print(record['post']['ts'])
                records.append(record)
           
    return reversed(records)  # records[0] oldest


def scrape(symbol, index_name, timeout,pages):
    scrape_cnt = 0
    while True:
        records = fetch(symbol,pages)
        curr_time = datetime.now()
        num_posts_added = 0
        last_saved_post = get_last_saved_post(index_name)
        print(last_saved_post)
        for record in records:
            
            try:
                if not last_saved_post or record['post']['ts'] > last_saved_post['ts']:
                    _ = client.index(index=index_name, id=record['id'], body=record['post'])
                    num_posts_added += 1
                    print(last_saved_post['ts'], record['post']['ts'])
                
            except:
                pass

        log_msg = 'Scrape: {} Current time: {} Added: {} Index: {}'.format(
            scrape_cnt,
            curr_time,
            num_posts_added,
            index_name
        )
        print(log_msg)
        time.sleep(timeout)
        scrape_cnt += 1
        if num_posts_added != 0:
            df = pd.DataFrame(records)
            df.to_csv(f'{symbol}.csv')
        else:
            pass



@click.command()
@click.option('--symbol', type=str, default="eosusdt")
@click.option('--timeout', type=int, default=5)
@click.option('--pages',type=int,default=1)
def main(symbol, timeout,pages):
    index_name = 'tradingview_{}'.format(symbol)
    try:
        index_exists_check(index_name)
    except ValueError:
        create_tradingview_index(index_name='tradingview_{}'.format(symbol))
        
    records = scrape(symbol, index_name, timeout,pages)
if __name__ == "__main__":
    main()

