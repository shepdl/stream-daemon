import Project
import riak

import sys

ps = Project.ProjectStore(riak.RiakClient())

# ps.create("UCLA vs. USC", ["ucla", "usc",])
# ps.create("Egypt", ["egypt",])

ps.create(sys.argv[1], sys.argv[2].split(","))