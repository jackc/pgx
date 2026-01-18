FROM mcr.microsoft.com/devcontainers/go:2-1.25-trixie

RUN apt-get update && apt-get install -y postgresql-common \
    && yes | /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh \
    && apt-get install -y postgresql-client-18 \
    && rm -rf /var/lib/apt/lists/*

USER vscode
RUN go install golang.org/x/tools/cmd/goimports@latest
