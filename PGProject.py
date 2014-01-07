import datetime
import logging

import simplejson as json

import psycopg2
import psycopg2.extras

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)

class Project(object):
    def __init__(self, name, keywords, db, active=True, id_key=None, created_at=None):
        self.message_count = 100
        self.name = name
        if created_at is None:
            created_at = datetime.datetime.now()
        self.created_at = created_at
        self.initial_keywords = keywords
        if type(keywords) == str or type(keywords) == unicode:
            if "," in keywords:
                keywords = keywords.split(",")
            else:
                keywords = [keywords]
        self.watching = keywords
        self.active = active
        self.id = id_key
        self.keyword_counts = dict(map(lambda k: (k, self.message_count,), keywords))
        self.logger = logging.getLogger()
        self.db = db
        self.cursor = db.cursor()

    def to_dict(self):
        return {
            "name" : self.name,
            "initial_keywords" : self.initial_keywords,
            "watching" : self.watching,
            "active" : self.active,
        }

    @classmethod
    def from_db(cls, record, db):
        return Project(record["name"], record["watching"], db, active=record["active"], 
            id_key=record["id"], created_at=record["created_at"])

    def calculate_new_keywords(self, algorithm):
        self.watching = algorithm(self.keyword_counts, self.message_count)
        return self.watching

    def reset(self):
        self.message_count = 0
        self.keyword_counts = {}

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
        self.cursor.execute(
            """ UPDATE projects SET watching = %s WHERE id = %s """, 
            (",".join(self.watching), self.id,)
        )
        return passing_hashtags

class ProjectStore(object):
    def __init__(self, db=None, pg_connection_string=""):
        super(ProjectStore, self).__init__()
        if db is None:
            if pg_connection_string == "":
                pg_connection_string = "dbname=poirot user=postgres"
            db = psycopg2.connect(pg_connection_string)
            db.set_client_encoding("UTF8")
        self.db = db
        self.projects = {}
        self.active_projects = {}

    def cursor(self):
        return self.db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def load_from_db(self):
        cursor = self.cursor()
        cursor.execute("SELECT * FROM projects;")
        while True:
            row = cursor.fetchone()
            if not row:
                break
            project = Project.from_db(row, self.db)
            self.projects[project.id] = project
            if project.active:
                self.active_projects[project.id] = project

    def create(self, name, keywords, id_key=None):
        project = Project(name, keywords, self.db, id_key=id_key)

        if id_key is None:
            cursor = self.cursor()
            cursor.execute(""" INSERT INTO projects(name, initial_keywords, active, watching, created_at) 
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, 
                (project.name, project.initial_keywords, project.active, project.watching, project.created_at,)
            )
            id_key = cursor.fetchone()[0]
            self.db.commit()
            cursor.close()
            project.id = id_key
        self.projects[id_key] = project
        self.active_projects[id_key] = project
        return project

    def stop(self, id_key):
        self.projects[id_key].active = False
        cursor.execute(""" UPDATE projects SET active = false WHERE id = %s """, (id_key,))
        del(self.active_projects[id_key])

    def list(self):
        return self.active_projects
