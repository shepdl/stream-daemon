__author__="daveshepard"
__date__ ="$Jun 10, 2011 4:11:10 PM$"

import MySQLdb
import threading
import time
import simplejson

import logging

host = "localhost"
username = "root"
password = "GIS4ucla"
database = "hcnow"

TWITTER_TABLE = "tweets"

#connection = None

def get_connection():
    connection = MySQLdb.connect(
        host=host,user=username,passwd=password,db=database,
        use_unicode=True,charset="utf8"
    )
    connection.set_character_set("utf8")
    return connection

def get_cursor(conn=None):
    if conn is not None:
        return conn.cursor(MySQLdb.cursors.DictCursor)
    return get_connection().cursor(MySQLdb.cursors.DictCursor)

class TweetStore(threading.Thread):
    TWEET_TABLE = 'tweets'
    def __init__(self, commit_threshold=10, interval=60, daemon=True):
        self.commit_threshold = commit_threshold
        self.tweets = []
        self.interval = interval
        self.logger = logging.getLogger('database')
        def generator(key):
            return lambda item: item[key]
        self.field_correlations = {
            'from_user_id': '',
            'profile_image_url': None,
        }
        threading.Thread.__init__(self)
    def log(self, message): 
        print "[Database] " + message

        #self.logger.info({'query_url_id': 'Database', 'message': message})
        #self.logger.info(message,extra={ 'query_url_id':'Database'})
        # self.logger.info("[Database] " + message)
    def add_tweets(self, items):
        self.tweets.append(items)
    def run(self):
        self.log("Starting TweetStore")
        time.sleep(10)
        self.running = True
        while self.running:
            self.commit()
            time.sleep(self.interval)
    def stop(self):
        self.running = False
        self.log("Stopping tweetstore thread from top stop.")
    def commit(self):
        conn = get_connection()
        for tweet_group in self.tweets:
            url_id = tweet_group["url_id"]
            self.log("Committing %s tweets for query_url_id %s" % (len(tweet_group["results"]), url_id))
            for tweet in tweet_group["results"]:
                fields = """query_url_id, from_user_id, profile_image_url, tweeted_at,
                            from_user, twitter_id, text, source, json"""
                value_string = u"""%s, %s, %s, %s, %s, %s,%s, %s, %s"""
                if "user" not in tweet:
                    tweet["user"] = {
                        "id_str" : "No ID",
                        "profile_image_url" : "nothing",
                        "screen_name": "userless tweet",
                    }
                values = (
                    url_id, tweet["user"]['id_str'],
                    tweet["user"]['profile_image_url'],
                    time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(tweet['created_at'],
                         '%a %b %d %H:%M:%S +0000 %Y'
                        )),
                    tweet["user"]['screen_name'], tweet['id_str'],
                    tweet['text'],
                    tweet['source'], simplejson.dumps(tweet),
                )
                if "location" in tweet:
                    fields += ", location"
                    value_string += ", %s"
                    values += (tweet['location'], )
                if "iso_language_code" in tweet["metadata"]:
                    fields += ", iso_language_code "
                    value_string += ", %s"
                    values += (tweet["metadata"]['iso_language_code'], )
                if "geo" in tweet and tweet["geo"]:
                    fields += ", reported_lat, reported_lon, reported_geometry, reported_geometry_pt"
                    value_string += u", %s, %s, %s, GeomFromText(%s)"
                    values += (
                        tweet['geo']['coordinates'][1], tweet['geo']['coordinates'][0],
                        "GeomFromText('POINT(%s %s)')" % (tweet['geo']['coordinates'][1],
                            tweet['geo']['coordinates'][0]),
                        #"GeomFromText('POINT(%s %s)')" % (tweet['geo']['coordinates'][1],
                        "POINT(%s %s)" % (tweet['geo']['coordinates'][1],
                            tweet['geo']['coordinates'][0]),
                    )
                if "in_reply_to_user_id_str" in tweet and tweet["in_reply_to_user_id_str"]:
                    fields += ", to_user_id"
                    value_string += ", %s"
                    values += (tweet['in_reply_to_user_id_str'], )
                if "retweet_id" in tweet and tweet["retweet_id"]:
                    fields += ", retweet_id"
                    value_string += ", %s "
                    values += (tweet["retweet_id"], )
                query = "INSERT INTO " + self.TWEET_TABLE + "(" + fields + ", created_at, updated_at) VALUES (" + value_string + ", NOW(), NOW())"
                cursor = conn.cursor(MySQLdb.cursors.DictCursor)
                cursor.execute(query, values)
                conn.commit()
            self.log("Commit finished")
        self.tweets = []
    def commit_safe(self):
        """ Saves tweets to database.
        
        Attempts thread safety using get_tweet_group
        """
        self.log("Doing safe commit")
        conn = get_connection()
        while True:
            tweet_group = self.get_tweet_group()
            if not tweet_group:
                break
            url_id = tweet_group["url_id"]
            for tweet in tweet_group["results"]:
                fields = """query_url_id, from_user_id, location, profile_image_url, tweeted_at,
                        from_user, twitter_id, text, iso_language_code, source"""
                value_string = u"""%s, %s, %s, %s, %s, %s,%s, %s, %s, %s"""
                values = (
                    url_id, tweet['from_user_id'], tweet['location'],
                    tweet['profile_image_url'],
                    time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(tweet['created_at'],
                        "%a, %d %b %Y %H:%M:%S +0000")), 
                    tweet['from_user'], tweet['id'],
                    tweet['text'],
                    tweet['iso_language_code'], tweet['source']
                )
                if "geo" in tweet and tweet["geo"]:
                    fields += ", reported_lat, reported_lon, reported_geometry_pt"
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
                query = "INSERT INTO " + self.TWEET_TABLE + "(" + fields + ", created_at, updated_at) VALUES (" + value_string + ", NOW(), NOW())"
                cursor = conn.cursor(MySQLdb.cursors.DictCursor)
                cursor.execute(query, values)
                conn.commit()
    def get_tweet_group(self):
        """ Returns tweet group. Operates destructively on list.
        """
        if len(self.tweets) > 0:
            tweet_group = self.tweets[0]
            self.tweets = self.tweets[1:]
            return tweet_group
        else:
            return None
    def stop(self):
        if len(self.tweets) > 0:
            self.commit()
        self.running = False
        # threading.Thread.stop()
        self.log("Stopping tweetstore thread from lower stop.")
