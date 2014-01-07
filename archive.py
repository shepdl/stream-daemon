import database
import urllib
import simplejson
import sys
import threading
import time
import re
import MySQLdb
from config import config
from twitter_local import Twitter, OAuth, TwitterHTTPError

from rest_reader import TweetThread

# we will create a tweet reader by extending TweetThread
# and re-implementing run to go in the opposite direction
# and use max_id
class Archiver(TweetThread):
    def __init__(self, url, url_id, max_id):
        store = database.TweetStore(commit_threshold=100)
        store.start()
        super(Archiver, self).__init__(url_id, url, store)
        self.max_id = max_id
        self.setDaemon(False)

    def run(self):
        no_results_counter = 0
        print "Hashtags: " + self.hashtags
        while self.running:
            if self.max_id is not None:
                cursor = database.get_cursor()
                query = "SELECT MIN(twitter_id) AS max_id FROM tweets WHERE query_url_id = %s" % (self.url_id)
                cursor.execute(query)
                max_id = cursor.fetchone()
                if max_id:
                    self.max_id = max_id = max_id['max_id']
                    self.log("Got twitter ID from database: %s" % max_id)
                cursor.close()  
            else:
                self.log("Got twitter ID from self: %s" % self.max_id)
                max_id = self.max_id
            tweets_retrieved = 0
            #log.write("Querying " + url)
            auth = OAuth(
                config["twitter_oauth_token"], config["twitter_oauth_token_secret"], 
                config["twitter_consumer_key"], config["twitter_consumer_secret"]
            )
            twitter = Twitter(auth=auth)
            while True:
                self.log("Querying %s in %s starting at %s" % (self.hashtags, self.geo, max_id,))
                try:
                    params = {
                        "q" : self.hashtags,
                        "count" : 100,
                    }
                    if max_id is not None:
                        params["max_id"] = max_id
                    if self.geo is not None:
                        params["geocode"] = self.geo
                    tweets = twitter.search.tweets(**params)
                except IOError, ex:
                    # The Twitter API can be flaky; just try again later.
                    print ex
                    self.log("IOError: %s" % (ex))
                    continue
                except TwitterHTTPError as ex:
                    print ex
                    print "Waiting 15 minutes in case of rate limiting ..."
                    time.sleep(15 * 60)
                    print "Resuming ..."
                    continue
                if tweets is None:
                    no_results_counter += 1
                    if no_results_counter >= 5:
                        print "Believe tweets are exhausted. Exiting ..."
                        self.running = False
                        break
                    time.sleep(5)
                elif "errors" in tweets:
                    print "Cute error message from Twitter: %s" % (result["error"])
                    print "Waiting 10 minutes in case of rate limiting ..."
                    time.sleep(600)
                elif "statuses" not in tweets:
                    print "Nothing from twitter...."
                    if no_results_counter < 5:
                        no_results_counter += 1
                        time.sleep(5)
                    else:
                        print "Believe tweets are exhausted. Exiting ...."
                        self.running = False
                        break
                elif "statuses" in tweets and len(tweets["statuses"]) == 0:
                    print tweets
                    print "No tweets found."
                    no_results_counter += 1
                    if no_results_counter >= 5:
                        print "Believe tweets are exhausted; exiting ..."
                        self.running = False
                        break
                    break
                no_results_counter = 0
                self.tweet_store.add_tweets({
                    "url_id" : self.url_id,
                    "results" : tweets["statuses"],
                })
                if "statuses" not in tweets or len(tweets["statuses"]) == 0:
                    break
                self.max_id = max_id = tweets["statuses"][-1]["id"] - 1
                tweets_retrieved += len(tweets["statuses"])
                print tweets["statuses"][-1]["created_at"]
        self.tweet_store.stop()
        sys.exit()
        

def read_tweets(url, url_id, id, sleep_interval=None):
    print "Reading tweets ..."
    #store = database.TweetStore(interval=10)
    #store.start()
    if id is None:
        id = 1
    no_result_counter = 0
    while id:
        if id == 1:
            id = None
        query_url = url
        if id is not None:
            query_url += "&max_id=%s" %(id,)
#       print query_url
        try:
                result = get_json(query_url)
        except IOError:
            print "Socket timeout or other IOError. Pausing for 5 seconds ...."
            time.sleep(5)
            continue
        except AttributeError:
            time.sleep(5)
        #print query_url
        if result is None:
                time.sleep(5)
            #   break
        elif "error" in result:
                print "Cute error message from Twitter: %s" % (result["error"])
                # TODO: sleep
                id = None
                break
        elif "results" not in result:
            print "Nothing from twitter...."
            if no_result_counter < 5:
                no_result_counter += 1
                time.sleep(5)
            else:
                print "Believe tweets are exhausted. Exiting ...."
                break
        elif "results" in result and len(result["results"]) == 0:
            print "No tweets found."
            break
        else:
            no_result_counter = 0
            id = result['results'][-1]['id'] - 1
            #store.add_tweets({
            #   "url_id": sys.argv[1],
            #   "results": result['results'],
            #})
            #print result
            #sys.exit()
            for tweet in result["results"]:
                fields = """query_url_id, from_user_id, profile_image_url, tweeted_at,
                        from_user, twitter_id, text, source"""
                value_string = u"""%s, %s, %s, %s, %s, %s,%s, %s"""
                print tweet['created_at']
                values = (
                    url_id, tweet['from_user_id'],
                    tweet['profile_image_url'],
                    time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(tweet['created_at'],
                        "%a, %d %b %Y %H:%M:%S +0000")),
                    tweet['from_user'], tweet['id'],
                    tweet['text'],
                    tweet['source']
                )
                if "location" in tweet:
                    fields += ", location"
                    value_string += ", %s"
                    values += (tweet['location'], )
                if "iso_language_code" in tweet:
                    fields += ", iso_language_code "
                    value_string += ", %s"
                    values += (tweet['iso_language_code'], )
                if "geo" in tweet and tweet["geo"]:
                    fields += ", reported_lat, reported_lon, reported_geometry"
                    value_string += u", %s, %s, %s"
                    values += (
                        tweet['geo']['coordinates'][1], tweet['geo']['coordinates'][0],
                        "GeomFromText('POINT(%s,%s)')" % (tweet['geo']['coordinates'][1],
                            tweet['geo']['coordinates'][0]),
                    )
                if "to_user_id" in tweet and tweet["to_user_id"]:
                    fields += ", to_user_id"
                    value_string += ", %s"
                    values += (tweet['to_user_id'], )
                if "retweet_id" in tweet and tweet["retweet_id"]:
                    fields += ", retweet_id"
                    value_string += ", %s "
                    values += (tweet["retweet_id"], )
                query = "INSERT INTO " + ' tweets ' + "(" + fields + ", created_at, updated_at) VALUES (" + value_string + ", NOW(), NOW())"
                conn = database.get_connection()
                cursor = conn.cursor(MySQLdb.cursors.DictCursor)
                cursor.execute(query, values)
                conn.commit()
            print result['results'][-1]['created_at'] + " " + result['results'][-1]['text']                     
        if sleep_interval is not None:
            time.sleep(sleep_interval)

def archive(item, id = None):
    print "Starting to archive ..."
    conn = database.get_connection()
    cursor = database.get_cursor(conn)
    cursor.execute("SELECT url FROM query_urls WHERE id = %s", (item,))
    result = cursor.fetchone()
    cursor.close()
    url = result['url']
    if id is None:
        cursor = database.get_cursor()
        cursor.execute("SELECT min(twitter_id) AS twitter_id FROM tweets WHERE query_url_id = %s", (item, ))
        result = cursor.fetchone()
        if result is not None:
            id = result['twitter_id']
    conn.close()
    archiver = Archiver(url, item, id)
    archiver.run()
    # read_tweets(url, item, id)

if __name__ == "__main__":
    twitter_id = None
    if len(sys.argv) > 2:
        twitter_id = sys.argv[2]
    archive(sys.argv[1],twitter_id) 
