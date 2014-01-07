# note: general dependencies
# python-daemon
# python-twitter
# riak


import daemon
import lockfile

from config import config

from Monitor import Monitor

context = daemon.DaemonContext(
        working_directory="/data/log/",
        umask=0o002,
        pidfile=lockfile.FileLock(config["pidfile"]),
        # pidfile=open(config["pidfile"], "w"),
        stdout=open(config["debug_log"], "w"),
        stderr=open(config["error_log"], "w"),
    )

monitor = Monitor()

with context:	
	monitor.run() 