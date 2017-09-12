# SQL Importer

Import a CSV file into Postgres with automatic column typing and table creation.

Features:

- Type inference for numbers, dates, datetimes, and booleans
- Automatic table creation
- Uniqueness and not null detection
- Automatic decompressing of gzip and bzip2 files
- Support for append instead of replace
- Support for CSV files wider than 1600 columns (the Postgres limit)

## Install

[Download a pre-built release](https://github.com/chop-dbhi/sql-importer/releases).


Or install it from source (requires Go).

```
go get github.com/chop-dbhi/sql-importer/cmd/sql-importer
```

## Usage

Specify the database URL and a CSV file to import. The table name will be derived from the filename by default.

```
sql-importer -db postgres://127.0.0.1:5432/postgres data.csv
```

See other options by running `sql-importer -h`.

## Status

Beta, works as expected. Command line options will likely change.

## License

MIT
