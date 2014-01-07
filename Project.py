import datetime
import logging

import riak

class Project(object):
    def __init__(self, name, keywords, message_bucket, active=True, id_key=None, 
        riak_record=None, created_at=None):
        self.message_count = 100
        self.name = name
        if created_at is None:
            created_at = datetime.datetime.now()
        self.created_at = created_at
        if type(keywords) == str:
            keywords = keywords.split(",")
        self.db = self.message_bucket = message_bucket
        self.initial_keywords = keywords
        if riak_record is not None:
            riak_data = riak_record.get_data()
            self.watching = riak_data["watching"]
            self.events = riak_data["events"]
        else:
            self.watching = keywords
            self.events = []
        self.active = active
        self.id = id_key
        self.keyword_counts = dict(map(lambda k: (k, self.message_count,), self.watching))
        self.riak_record = riak_record
        self.logger = logging.getLogger()

    def to_dict(self):
        return {
            "name" : self.name,
            "created_at" : self.created_at.isoformat(),
            "initial_keywords" : self.initial_keywords,
            "watching" : self.watching,
            "active" : self.active,
            "events" : self.events,
        }

    @classmethod
    def from_db(cls, record, message_bucket):
        data = record.get_data()
        return Project(data["name"], data["watching"], message_bucket, 
            active=data["active"], id_key=record.get_key(), riak_record=record)

    def calculate_new_keywords(self, algorithm):
        self.watching = algorithm(self)
        return self.watching

    def reset(self):
        self.message_count = 0
        self.keyword_counts = {}

    def commit(self):
        self.riak_record.set_data(self.to_dict())
        self.riak_record.store()

    def examine_message(self, keywords):
        found_keywords = filter(lambda k: k in self.watching, keywords)
        if len(found_keywords) > 0:
            self.message_count += 1
            for keyword in keywords:
                if keyword not in self.keyword_counts:
                    self.keyword_counts[keyword] = 0
                self.keyword_counts[keyword] += 1
            return True
        else:
            return False

    def filter_hashtags(self, algorithm):
        passing_hashtags = algorithm(self)
        # write out project
        self.watching = passing_hashtags
        self.events.append({
            "type" : "recalculate_hashtags",
            "time" : datetime.datetime.now().isoformat(),
            "result" : self.watching,
        })
        self.riak_record.set_data(self.to_dict())
        self.riak_record.store()
        return passing_hashtags

class ProjectStore(object):
    def __init__(self, db=None):
        super(ProjectStore, self).__init__()
        if db is None:
            db = riak.RiakClient(port=8087, transport_class=riak.RiakPbcTransport)
        self.db = db
        self.projects = {}
        self.active_projects = {}

    def load_from_db(self):
        project_bucket = self.db.bucket("project")
        for key in project_bucket.get_keys():
            riak_project = project_bucket.get(key)
            project = Project.from_db(riak_project, self.db.bucket("message"))
            self.projects[riak_project.get_key()] = project
            if project.active:
                self.active_projects[riak_project.get_key()] = project


    def create(self, name, keywords, bucket=None, id_key=None):
        if bucket is None:
            bucket = self.db.bucket("project")
        message_bucket = self.db.bucket("message")
        project = Project(name,keywords,message_bucket=message_bucket,id_key=id_key)

        if id_key is None:
            riak_project = bucket.new(data=project.to_dict())
            riak_project.store()
            project.riak_record = riak_project
            id_key = riak_project.get_key()
            project.id = id_key

        self.projects[id_key] = project
        self.active_projects[id_key] = project
        return project, id_key

    def stop(self, id_key):
        if id_key not in self.projects:
            return {"error" : "Project not found."}
        elif id_key not in self.active_projects:
            return {"error" : "Project is not active."}
        else:
            project = self.projects[id_key]
            project.active = False
            project.commit()
            del(self.active_projects[id_key])
            print "Stopping project"
            return {
                "id" : project.id,
                "name" : project.name,
                "message" : "Project %s (with id %s) stopped." % (
                    project.name,
                    project.id,
                )
            }

    def list(self):
        return self.active_projects
