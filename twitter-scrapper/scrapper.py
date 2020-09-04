import argparse
import csv
import datetime
import json
import logging
import re
from argparse import Namespace
from google.cloud import pubsub_v1

import tweepy

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', required=False, type=int, default=100)
    parser.add_argument('--query', required=True, nargs="+")
    parser.add_argument('--output-file', required=False)
    parser.add_argument('--projectId', required=False, default='divine-bloom-279918')
    parser.add_argument('--topic', required=False, default='tweets')
    return parser.parse_args()


def init_twitter_api() -> tweepy.API:
    with open('secrets.json', 'r') as f:
        secrets = json.load(f)
        auth = tweepy.OAuthHandler(secrets['consumer_key'], secrets['consumer_key_secret'])
        auth.set_access_token(secrets['access_token'], secrets['access_token_secret'])
        return tweepy.API(auth, wait_on_rate_limit=True)


def clean_text(text):
    text = re.sub('\n', ' ', text)
    text = re.sub('\t', ' ', text)
    text = re.sub(' +', ' ', text)
    return text


def convert_datetime(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


def write_to_file(opts: Namespace, tweets):
    tweets_list = [[t.author.name,
                    t.place,
                    t.coordinates,
                    t.created_at,
                    t.text,
                    t.source,
                    t.id] for t in tweets]

    with open(opts.output_file, "w") as f:
        writer = csv.writer(f)
        writer.writerows(tweets_list)


def run_scrapper(opts: Namespace):
    print(opts)

    text_query = " ".join(opts.query)
    count = opts.count

    topic_name = f'projects/{opts.projectId}/topics/{opts.topic}'

    api = init_twitter_api()
    publisher = pubsub_v1.PublisherClient()

    tweets = tweepy.Cursor(api.search, q=text_query).items(count)
    for t in tweets:
        message = {
            "author": t.author.name,
            "text": t.text,
            "id": t.id,
            "createdAt": t.created_at,
            "source": t.source,
            "place": t.place,
            "coordinates": t.coordinates
        }

        message = json.dumps(message, default=convert_datetime)
        print(f"Sending message: {message}")
        publisher.publish(topic_name, data=message.encode())

    if opts.output_file:
        write_to_file(opts, tweets)


def main():
    opts = parse_args()
    run_scrapper(opts)


if __name__ == '__main__':
    main()
