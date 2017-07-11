# SQL Importer

Import a CSV file into Postgres with automatic column typing, table creation, and unique and not null constraints.

## Install

```
go get github.com/chop-dbhi/sql-importer
```

## Usage

Specify the database URL and a CSV file to import. The table name will be derived from the filename by default.

```
sql-importer -db 127.0.0.1:5432/postgres data.csv
```

See other options by running `sql-importer -h`.

## Status

Beta, works as expected. Command line options will likely change.

## License

MIT
