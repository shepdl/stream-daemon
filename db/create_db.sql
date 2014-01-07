CREATE DATABASE stream_daemon;

CREATE TABLE messages (
    id INTEGER NOT NULL PRIMARY KEY SERIAL,
    source_id VARCHAR(100), -- ID from service
    username VARCHAR(50),
    content TEXT,
    raw_data TEXT,
    sent_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE tweets (
    from_user: VARCHAR(50),
    from_user_id: VARCHAR(50),
    geo_lat: DOUBLE,
    geo_lon: DOUBLE,
    location: VARCHAR(50),
    iso_language_code: VARCHAR(2),
    retweet_count: INTEGER,
    profile_image_url: VARCHAR(100),
    source: VARCHAR(200),
    to_user: VARCHAR(50),
    to_user_id: VARCHAR(50)
) INHERITS (messages);

CREATE TABLE service_users (
    name varchar(255),
    source varchar(255)
);

create table tags (
    name varchar(255) unique primary key
);

create table message_tags (
    message_id integer references messages id,
    tag_id integer references tags id
);

create table local_users (
    id serial primary key not null,
    name varchar(255),
    twitter_secret_key varchar(255)
);

create table communities (
    id serial primary key not null,
    began_at timestamp with time zone,
    ended_at timestamp with time zone null,
    owner_id references local_users id,
    original_tags text,
    active boolean
);

create table tag_snapshot (
    tag_name references tags name,
    start_at timestamp with time zone,
    ends_at timestamp with time zone
);
