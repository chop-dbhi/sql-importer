FROM alpine:3.6

COPY ./dist/linux-amd64/sql-importer /

ENTRYPOINT ["/sql-importer"]
