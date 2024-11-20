# Common SQL Connectors

A package for common connectors strategies in java.

I'm not sure if connector qualifies as a design pattern.
From practical experiences, I've learned people refer to it
as synchronizing 2 or more data sources.

Commonly I'm found in a situation where I have to implement
database synchronization through some form of "dump" from 
some source connection into a cloud storage, and later
a "sink" connection would read those dump outputs in order
to import that.
Usually I would resort to some sort of open source, battle
tested solution such as kafka connectors, ...;

But sometimes people would rather not use any additional 
infrastructure besides their already existing databases
and cloud storages.

Common SQL Connectors are java implementation from those
repetitive architectural patterns I found.

## Strategies

### Postgres -> S3 -> Postgres


### Oracle -> S3 -> Postgres