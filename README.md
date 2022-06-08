# Flink OSS Connector

[Flink](https://github.com/apache/flink) SQL connector for [OSS](https://help.aliyun.com/document_detail/31947.html?spm=5176.8041989.303605.24.330be8f69SZ39E) database,
this project Powered by [OSS Java SDK](https://help.aliyun.com/document_detail/32007.html). This is a connector that implements the most basic functions. Better and richer functions can be added on the basis of this connector.

## Connector Options

| Option                     | Required | Default  | Type     | Description                                                                                    |
|:---------------------------| :------- |:---------|:---------|:-----------------------------------------------------------------------------------------------|
| endpoint                   | required | none     | String   | Endpoint indicates the OSS external service access domain name.                                |
| access-key-id              | required | none     | String   | AccessKey refers to the accessKeyId and accessKeySecret used in access authentication.         |
| access-key-secret          | required | none     | String   |                                                                                                |
| bucket-name                | required | none     | String   | Storage space is a container used by users to store objects.                                   |
| object-name                | required | none     | String   | Objects are the basic units of OSS data storage, also known as OSS files.                      |
| format                     | required | none     | String   | Flink provides a set of [table formats](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/formats/overview/) that can be used with table connectors.                          |

## How to use

### Read a csv file on OSS

```SQL
CREATE TABLE sourceTable (
    name STRING,
    age INT)
WITH (
  'connector' = 'oss',
  'endpoint' = '<your oss endpoint>',
  'access-key-id' = '<your oss accessKeyId>',
  'access-key-secret' = '<your oss accessKeySecret>',
  'bucket-name' = '<your oss bucketName>',
  'object-name' = '<your oss objectName>>',
  'format' = 'csv'
);

CREATE TABLE sinkTable (
    name VARCHAR,
    age INT
) WITH (
    'connector' = 'print'
);

INSERT INTO sinkTable(name,age) SELECT name,age FROM sourceTable limit 10;
```

### Write a CSV file to OSS
```SQL
CREATE TABLE sourceTable (
    f_random INT,
    f_random_str STRING
    ) WITH (
    'connector' = 'datagen',
    'rows-per-second'='1',
    'number-of-rows'='10',
    'fields.f_random.max'='100',
    'fields.f_random.min'='0',
    'fields.f_random_str.length'='3'
);


CREATE TABLE sinkTable (
    name STRING,
    age INT)
WITH (
  'connector' = 'oss',
  'endpoint' = '<your oss endpoint>',
  'access-key-id' = '<your oss accessKeyId>',
  'access-key-secret' = '<your oss accessKeySecret>',
  'bucket-name' = '<your oss bucketName>',
  'object-name' = '<your oss objectName>>',
  'format' = 'csv',
  'csv.field-delimiter' = ','
);

INSERT INTO sinkTable(name,age) SELECT f_random_str,f_random FROM sourceTable;
```