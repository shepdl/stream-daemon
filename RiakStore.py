from Store import Store

class TweetStore(Store):
    def __init__(self, project):
        # super(Store, self).__init__(project)
        Store.__init__(self, project)

    def commit(self):
        print "Committing for %s" % (self.project.name,)
        for message in self.message_cache:
            riak_message = self.db.new(data=message)
            riak_message.add_link(self.project.riak_record)
            riak_message.store()
            self.project.riak_record.add_link(riak_message)
            self.project.riak_record.store()
        self.message_cache = []

    def store(self, data):
        # print "Message for %s" % (self.project.name,)
        self.message_cache.append(data)
        if len(self.message_cache) >= self.commit_limit:
            self.commit()
