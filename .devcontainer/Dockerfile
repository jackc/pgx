FROM mcr.microsoft.com/devcontainers/base:ubuntu24.04

RUN apt-get update && apt-get install -y postgresql-common \
    && yes | /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh \
    && apt-get install -y postgresql-client-18 \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update \
    && apt-get install -y build-essential libssl-dev libreadline-dev zlib1g-dev libffi-dev ncurses-dev libyaml-dev libxml2 libxml2-dev libxslt1-dev -y postgresql-client-18 \
    && rm -rf /var/lib/apt/lists/*
