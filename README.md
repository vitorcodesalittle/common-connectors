# Common SQL Connectors

Simple and repeated patterns to synchronize data sources.

## How to run:

### Oracle Source Connector

How to upload Oracle data to S3 in CSV format:

```bash
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
./connectors source \
  --dataSourceUrl=oracle:thin:.... \
  --user=SYSADMIN \
  --password=YOUR_PASSWORD_HERE \
  --awsS3Bucket=YOUR_TARGET_BUCKET_HERE
```

### Postgres Sink Connector

How to sink AWS CSV. The command will update the schema as necessary (this can be turned off with `--updateSchema`).


```bash
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
./connectors sink \
  --dataSourceUrl=oracle:thin:.... \
  --user=YOUR_USER \
  --password=YOUR_PASSWORD_HERE \
  --awsS3Bucket=YOUR_TARGET_BUCKET_HERE \
  --incremental=false \
  --updateSchema=false
```

## Schema changes

1. Source adds Column

Then Sink should add the column.

2. Source changes column data type

Then Sink should change the data type if possible.

3. Source removes column

Then Sink should do nothing.

## Strategies

### Postgres -> S3 -> Postgres


### Oracle -> S3 -> Postgres