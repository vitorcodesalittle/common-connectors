# Common SQL Connectors

Simple and repeated patterns to synchronize data sources.

## How to run:

### Oracle Source Connector

How to upload Oracle data to S3 in CSV format:

In both examples you need a file containing all query to extract the data.
Just SourceConnector uses the query, SinkConnector refers to this file to 
find the external CSV objects.

```json
[
  {
    "query" : "SELECT * FROM hr.d;",
    "schemaName" : "hr",
    "tableName" : "d"
  }
]
```

```bash
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
./connectors source \
  --dataSourceUrl=oracle:thin:.... \
  --user=SYSADMIN \
  --password=YOUR_PASSWORD_HERE \
  --awsS3Bucket=YOUR_TARGET_BUCKET_HERE \
  --etl-file=./etl-file.json
```

### Postgres Sink Connector

How to sink AWS CSVs.

```bash
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
./connectors sink \
  --dataSourceUrl=oracle:thin:.... \
  --user=YOUR_USER \
  --password=YOUR_PASSWORD_HERE \
  --awsS3Bucket=YOUR_TARGET_BUCKET_HERE \
  --etl-file=./etl-file.json
```

## Caveats

### Data Type Caveats

Just simple data types are supported. If data is not int, double, text, timestamps, dates, etc...
you must cast it to the closer type available.

Example data types not supported:
- XML
- Currency
- Tuples

### Foreign Key Caveats

We don't compute the correct table order for insert respecting FKs. Therefore this
only works if you're working on tables that are not related to each other.
This can be addressed in the future.

### Sink Data Source DDL Caveats

We don't automatically try to update the sink data source schema. Therefore, it should
match the source schema as a requisite.
This can be addressed in the future.

#### Schema changes

1. Source adds Column

Then Sink should add the column.

2. Source changes column data type

Then Sink should change the data type if possible.

3. Source removes column

Then Sink should do nothing.

## Strategies

### Postgres -> S3 -> Postgres


### Oracle -> S3 -> Postgres