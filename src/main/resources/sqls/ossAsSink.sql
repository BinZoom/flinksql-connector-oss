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