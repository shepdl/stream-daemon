Stream Daemon
=============

Hashtag-based twitter sampling program. Queries Twitter for tweets that mention certain hashtags, and stores the returned tweets.

To use stream daemon, first create an application with Twitter. Make a note of your consumer key, consumer secret, oauth token, and oauth token secret. Copy config_template.py to config.py, and fill in these values, along with your PostgreSQL database credentials, in config.py. 


After that, you're ready to go. Stream Daemon can sample both the REST API and the straeming API.

1. To use the REST API, use rest_reader.py. You can also archive tweets as far back as Twitter will let you with archive.py; just get the ID of the query you created from the database and run python archive.py {id}.

2. To use the streaming API, run start_daemon.py. The streaming daemon operates adaptively, and gathers hashtags strongly associated with the hashtags you submit. The REST API just uses the hashtags you submitted.


Status messages are written to the log files listed in config.py.
