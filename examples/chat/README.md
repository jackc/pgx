# Description

This is a sample chat program implemented using PostgreSQL's listen/notify
functionality with pgx.

Start multiple instances of this program connected to the same database to chat
between them.

## Connection configuration

The database connection is configured via enviroment variables.

* CHAT_DB_HOST - defaults to localhost
* CHAT_DB_USER - defaults to current OS user
* CHAT_DB_PASSWORD - defaults to empty string
* CHAT_DB_DATABASE - defaults to postgres

You can either export them then run chat:

    export CHAT_DB_HOST=/private/tmp
    ./chat

Or you can prefix the chat execution with the environment variables:

    CHAT_DB_HOST=/private/tmp ./chat
