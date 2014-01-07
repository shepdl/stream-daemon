from threading import Thread
import logging
import simplejson as json
import time

from twitter_local import TwitterStream, UserPassAuth, OAuth, TwitterHTTPError

# from RiakStore import TweetStore
from PGStore import TweetStore
from config import config

class TweetListener(Thread):

    def __init__(self, project_list, config_object):
        super(TweetListener, self).__init__(name="TweetListener")
        self.project_list = project_list
        self.username = config_object["username"]
        self.password = config_object["password"]
        self.running = True
        self.project_stores = {}
        for id, project in project_list.active_projects.iteritems():
            self.project_stores[project.id] = TweetStore(project)

    def start(self, keywords):
        self.keywords = keywords
        if not self.running:
            self.running = True
        super(TweetListener, self).start()

    def run(self):
        logger = logging.getLogger(__name__)
        #auth = OAuth(config["twitter_oauth_token"], config["twitter_oauth_token_secret"], 
        #        config["twitter_consumer_key"], config["twitter_consumer_secret"])
        auth = UserPassAuth(self.username, self.password)
        # streamer = TwitterStream()
        streamer = TwitterStream(auth=auth)
        # iterator = streamer.statuses.filter(track=",".join(self.keywords))
        print "Tracking %s" % (", ".join(self.keywords),)
        iterator = streamer.statuses.filter(track=",".join(map(lambda s: s.encode('utf-8'), self.keywords)))
        while self.running:
            try:
                for tweet in iterator:
                    if not self.running:
                        break
                    if tweet is not None and "text" in tweet:
                        # logger.debug(tweet["text"])
                        if not self.running:
                            break
                        hashtags = map(lambda t: (t["text"].lower()), tweet["entities"]["hashtags"])
                        for id, project in self.project_list.active_projects.iteritems():
                            # print "Checking against %s" % (project.name,)
                            if project.examine_message(hashtags):
                                # print "Found tweet for %s " % (project.name,)
                                if id not in self.project_stores:
                                    self.project_stores[id] = TweetStore(project)
                                self.project_stores[id].store(tweet)
                        time.sleep(0.0001)        
            except TwitterHTTPError, ex:
                logger.exception(ex)
                print ex
        # commit before everything is done
        for id in self.project_list.active_projects:
            self.project_stores[id].commit()

    def stop(self):
        self.running = False
