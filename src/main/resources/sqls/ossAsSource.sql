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