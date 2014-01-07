class Store(object):
    """
    A master StorePool object provides Stores that administer saving data.
    Each XXListener creates a series of XXStore objects that caches and stores data.
    If Project.examine_message returns True, XXStore stores the data.
    """
    def __init__(self, project, commit_limit=None):
        if commit_limit is None:
            commit_limit = 500
        self.project = project
        self.db = project.db
        self.message_cache = []
        self.commit_limit = commit_limit
