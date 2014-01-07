# PGStore.py
import sys
import datetime

from Store import Store
import simplejson as json

class TweetStore(Store):
    def __init__(self, project):
        # super(Store, self).__init__(project)
        Store.__init__(self, project)
        self.start_time = datetime.datetime.now()
        self.counter = 0

    def commit(self):
        print "committing for project %s after %s seconds of streaming ... " % (self.project.name, (datetime.datetime.now() - self.start_time),)
        geo_items = []
        non_geo_items = []
        commit_start = datetime.datetime.now()
        for message in self.message_cache:
            geo = message["geo"]
            list = non_geo_items if geo is None else geo_items
            if geo is not None:
                geo = "POINT(%s %s)" % (message["coordinates"]["coordinates"][0], 
                    message["coordinates"]["coordinates"][1],
                )
            list.append((
                    message["id_str"], message["user"]["screen_name"], 
                    message["text"], json.dumps(message),
                    message["created_at"], message["user"]["id_str"],
                    geo, message["user"]["location"], message["user"]["lang"], 
                    message["retweet_count"], message["in_reply_to_user_id_str"], 
                    message["in_reply_to_screen_name"],
                    self.project.id,
                ))
        non_geo_query = """ INSERT INTO tweets(source_id, username, content, raw_data,
                    sent_at, from_user_id, geo, location, iso_language_code, retweet_count,
                    to_user, to_user_id, project_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
        """
        geo_query = """ INSERT INTO tweets(source_id, username, content, raw_data,
                    sent_at, from_user_id, geo, location, iso_language_code, retweet_count,
                    to_user, to_user_id, project_id
                ) VALUES (%s,%s,%s,%s,%s,%s,ST_GeomFromText(%s),%s,%s,%s,%s,%s,%s) 
        """
        print "Commit complete in %s " % (datetime.datetime.now() - commit_start,)
        cursor = self.db.cursor()
        cursor.executemany(non_geo_query, non_geo_items)
        cursor.executemany(geo_query, geo_items)
        self.db.commit()
        cursor.close()
        self.message_cache = []
        self.counter = 0
        self.start_time = datetime.datetime.now()

    def store(self, data):
        self.message_cache.append(data)
        self.counter += 1
        if self.counter >= self.commit_limit:
            self.commit()
