-- based on postgres
create database crawler;
create role prop login with password '';
create table pages (
    id primary key serial,
    url text,
    status int,
    headers json,
    post_msg text,
    date_created timestamp default now(),
    date_updated timestamp
);
grant all on pages to prop;
grant all on pages_id_seq to prop;

create database crawler_test;
create table pages (
    id serial primary key,
    url text,
    status int,
    headers json,
    post_msg text,
    date_created timestamp default now(),
    date_updated timestamp
);
grant all on pages to prop;
grant all on pages_id_seq to prop;
