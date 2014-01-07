#! /usr/bin/python
from twitter_local import Twitter, OAuth

from daemon import runner
import sys
import datetime
import time
import urllib
import database
from config import config
import simplejson
import threading
from urlparse import urlparse, parse_qs

class TweetThread(threading.Thread):

    MAX_INTERVAL = 600 # 10 min is max wait time
    MIN_INTERVAL = 60

    def __init__(self, url_id, url, store):
        parsed_url = parse_qs(urlparse(url).query)
        self.hashtags = ""

        if "q" in parsed_url:
            self.hashtags = parsed_url["q"][0] # TODO: parse URL

        self.geo = None
        if "geocode" in parsed_url:
            self.geo = parsed_url["geocode"][0]

        self.url_id = url_id
        self.time_to_wait = self.MIN_INTERVAL
        self.num_tweets_retrieved = []
        self.last_run = datetime.datetime.now()
        self.running = True
        self.finished_saving = False
        self.tweet_store = store
        self.since_id = None

        threading.Thread.__init__(self)
        self.setDaemon(True)

    def log(self, message):
        print "[%s] %s" % (self.url_id, message,)

    def run(self):
        while self.running:

            if self.since_id is not None:
                cursor = database.get_cursor()
                query = "SELECT MAX(twitter_id) AS since_id FROM tweets WHERE query_url_id = %s" % (self.url_id)
                cursor.execute(query)
                since_id = cursor.fetchone()
                if since_id:
                    self.since_id = since_id = since_id['since_id']
                    self.log("Got twitter ID from database: %s" % since_id)
                cursor.close()  
            else:
                self.log("Got twitter ID from self: %s" % self.since_id)
                since_id = self.since_id

            tweets_retrieved = 0
            #log.write("Querying " + url)

            auth = OAuth(
                config["twitter_oauth_token"], config["twitter_oauth_token_secret"], 
                config["twitter_consumer_key"], config["twitter_consumer_secret"]
            )
            twitter = Twitter(auth=auth)

            while True:
                self.log("Querying %s in %s" % (self.hashtags, self.geo,))
                try:
                    tweets = twitter.search.tweets(
                        count=100,
                        q=self.hashtags,
                        geocode=self.geo,
                        since_id=self.since_id,
                    )
                except IOError, ex:
                    # The Twitter API can be flaky; just try again later.
                    self.log("IOError: %s" % (ex))
                    continue

                self.tweet_store.add_tweets({
                    "url_id" : self.url_id,
                    "results" : tweets["statuses"],
                })

                if "statuses" not in tweets or len(tweets["statuses"]) == 0:
                    break

                if tweets["statuses"][0]["id"] > self.since_id:
                    self.since_id = tweets["statuses"][0]["id"]

                self.log("Added %s tweets to store" % (len(tweets["statuses"]), ))
                tweets_retrieved += len(tweets["statuses"])

                if "next_page" not in tweets:
                    break
                else:
                    self.log("Next page: %s" % (url,))
            self.log("%s tweets found for url_id %s" % (tweets_retrieved, self.url_id))

            if "statuses" not in tweets or "error" in tweets or tweets_retrieved == 0:
                self.log("No results returned. Going to sleep for %s seconds." % self.MAX_INTERVAL)
                self.time_to_wait = self.MAX_INTERVAL
                if tweets_retrieved == 0:
                    self.log("No tweets found, but query worked. Going to sleep for %s seconds." % self.MAX_INTERVAL)

            elif self.time_to_wait < self.MAX_INTERVAL and tweets_retrieved < 100:
                self.time_to_wait = (2 * self.time_to_wait < self.MAX_INTERVAL) and 2 * self.time_to_wait or self.MAX_INTERVAL
                self.log(
                    "%s tweets found. Increasing time. Going to sleep for %s seconds" % (
                        tweets_retrieved, self.time_to_wait)
                )

            elif tweets_retrieved == 100:
                self.time_to_wait = 0
                self.log("100 tweets found. Reducing time to %s seconds." % (self.time_to_wait))
            self.log("Sleeping for %s" % (self.time_to_wait,))
            time.sleep(self.time_to_wait)

        self.finished_saving = True

    def stop(self):
        self.running = False
        self.log("Stopped tweet watcher thread for query_url_id %s." % (self.url_id,))


class TweetDaemon(object):
    def __init__(self):
        self.tweet_threads = {}
        self.store = database.TweetStore()
        self.stdin_path = '/dev/null'
        self.stdout_path = config['debug_log']
        self.stderr_path = config['error_log']
        self.pidfile_path = config['pidfile']
        self.pidfile_timeout = 5

    def log(self, message):
        print "[Master Daemon] " + message

    def run(self):
        self.log("Running daemon ...")
        self.store.start()

        while True:
            query = "SELECT id, url, is_archived FROM query_urls WHERE is_active = 1"
            conn = database.get_connection()
            cursor = database.get_cursor(conn)
            cursor.execute(query)
            active_ids = []

            # this loop spawns new threads after reading from the database
            while True:
                row = cursor.fetchone()
                if not row:
                    cursor.close()
                    break
                self.log("Found url %s at %s" % (row['id'], row['url']))
                active_ids.append(row['id'])
                if row['id'] not in self.tweet_threads:
                    self.log("Spawning thread for row %s \n" % (row['id']))
                    self.log("Spawing thread for query_url_id %s " % (row['id'],))
                    self.tweet_threads[row['id']] = TweetThread(row['id'], row['url'], self.store)
                    self.tweet_threads[row['id']].start()
            conn.close()

            for thread_id in self.tweet_threads.iterkeys():
                if thread_id not in active_ids:
                    self.log("Stopped thread %s" % (row['id'],))
                    self.tweet_threads[thread_id].stop()
                    del(self.tweet_threads[thread_id])
            if len(self.tweet_threads) == 0:
                break
            time.sleep(600)

    def stop(self):
        self.log("Stopping master daemon ...")
        for thread in self.tweet_threads:
            thread.stop()
        while True:
            running_threads = 0
            for thread in self.tweet_threads:
                if not thread.finished_saving:
                    running_threads += 1
            self.log("%s tweet readers still active ..." % (running_threads,))
            if running_threads == 0:
                break
            self.log("All threads stopped. Committing and stopping.")
        self.store.commit()
        self.log("Quitting ...") 
        Daemon.stop(self)


if __name__ == "__main__":
    app = TweetDaemon()
    daemon_runner = runner.DaemonRunner(app)
    daemon_runner.parse_args()
    daemon_runner.do_action()
    
