import argparse
import csv
import json
import time
from argparse import Namespace

import tweepy


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', required=False, type=int, default=100)
    parser.add_argument('--query', required=True, nargs="+")
    parser.add_argument('--output-file', required=False, default='output.txt')
    return parser.parse_args()


def init_api() -> tweepy.API:
    with open('secrets2.json', 'r') as f:
        secrets = json.load(f)
        auth = tweepy.OAuthHandler(secrets['consumer_key'], secrets['consumer_key_secret'])
        auth.set_access_token(secrets['access_token'], secrets['access_token_secret'])
        return tweepy.API(auth, wait_on_rate_limit=True)


def run_scrapper(opts: Namespace):
    print(opts)

    output_file = opts.output_file
    text_query = " ".join(opts.query)
    count = opts.count

    api = init_api()
    tweets = tweepy.Cursor(api.search, q=text_query).items(count)

    tweets_list = [[t.author.name,
                    t.place,
                    t.coordinates,
                    t.created_at,
                    t.text,
                    t.source,
                    t.id] for t in tweets]

    with open(output_file, "w") as f:
        writer = csv.writer(f)
        writer.writerows(tweets_list)


def main():
    opts = parse_args()
    run_scrapper(opts)


if __name__ == '__main__':
    main()
