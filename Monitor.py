#!/usr/bin/python
import datetime
import time
from threading import Thread

from config import config

from PGProject import ProjectStore
from TweetListener import TweetListener
from CommandServer import CommandServer

class Monitor(Thread):
    def __init__(self, refresh_period=None):
        if refresh_period is None:
            self.refresh_period = datetime.timedelta(minutes=15)
        super(Monitor, self).__init__(name="Monitor")
        # create project list
        self.project_list = ProjectStore() # ProjectStore(pg_connection_string=config["pg_connection_string"])
        self.project_list.load_from_db()
        self.running = True
        
        # command listener
        self.server = CommandServer(self, config)
        self.server.start()

    def run(self):
        while self.running:
            start_time = datetime.datetime.now()
            # compute desired hashtags
            print "Computing hashtags ..."
            desired_hashtags = list(set([el for item in map(
                lambda p: p.filter_hashtags(percentage_or_greater(0.01)), 
                # five_or_greater,
                self.project_list.active_projects.values()) for el in item ])
            )
            for project in self.project_list.active_projects.values():
                project.reset()
            calculation_time = datetime.datetime.now() - start_time
            # start reader object(s)
            self.readers = [TweetListener(self.project_list, config),]
            if desired_hashtags != []:
                if len(desired_hashtags) > 400:
                    desired_hashtags = desired_hashtags[0:400]
                for reader in self.readers:
                    reader.start(desired_hashtags)
            else:
                print "No hashtags found; sleeping ..."
            # sleep for interval minus last calculation time
            time.sleep((self.refresh_period - calculation_time).seconds)
            for reader in self.readers:
                reader.stop()
        self.server.stop()
        print "Shutting down monitor thread: falling off end of run() ..."

    def stop(self):
        self.running = False

def percentage_or_greater(percentage):
    def evaluator(project):
        return [k for (k,v) in project.keyword_counts.iteritems() if v / float(project.message_count) >= percentage 
            and "follow" not in k
        ]
    return evaluator


def one_or_greater(project):
    return [k for (k,v) in project.keyword_counts.iteritems() if v / float(project.message_count) >= 0.01]

def five_or_greater(project):
    # print "Project %s has %s hashtags: %s" % (project.name, len(project.keyword_counts.keys()), project.keyword_counts.keys()) 
    # print project.keyword_counts
    return [k for (k,v) in project.keyword_counts.iteritems() if v / float(project.message_count) >= 0.05]

def ten_or_greater(total_tweets):
    def fn(hashtag, count):
        return count / float(total_tweets) >= 0.10
    return fn


if __name__ == '__main__':
    mon = Monitor()
    mon.run()
