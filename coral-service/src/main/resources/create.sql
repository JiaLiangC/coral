CREATE TABLE list_bucket_single (key STRING, value STRING)
    SKEWED BY (key) ON (1,5,6) STORED AS DIRECTORIES;

CREATE TABLE list_bucket_multiple (col1 STRING, col2 int, col3 STRING)
    SKEWED BY (col1, col2) ON (('s1',1), ('s3',3), ('s13',13), ('s78',78)) STORED AS DIRECTORIES;