-- Rename Table
ALTER TABLE old_table_name RENAME TO new_table_name;

-- Alter Table Properties
ALTER TABLE table_name SET TBLPROPERTIES ('comment' = 'New table comment','b'='c');

-- Add SerDe Properties
ALTER TABLE table_name SET SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
    WITH SERDEPROPERTIES ('serialization.format' = ',', 'field.delim' = ',');

-- Remove SerDe Properties
ALTER TABLE table_name UNSET SERDEPROPERTIES ('field.delim');

-- Alter Table Storage Properties
ALTER TABLE table_name CLUSTERED BY (id) SORTED BY (name) INTO 5 BUCKETS;

-- Alter Table Skewed
ALTER TABLE table_name SKEWED BY (col1, col2)
    ON ((1,1), (2,2)) STORED AS DIRECTORIES;


ALTER TABLE my_table NOT CLUSTERED;

ALTER TABLE my_table NOT SORTED;

-- Alter Table Not Skewed
ALTER TABLE table_name NOT SKEWED;

-- Alter Table Not Stored as Directories
ALTER TABLE table_name NOT STORED AS DIRECTORIES;


ALTER TABLE list_bucket_single  SKEWED BY (key) ON (1,5,6) STORED AS DIRECTORIES;

ALTER TABLE list_bucket_multiple SKEWED BY (col1, col2) ON (('s1',1), ('s3',3), ('s13',13), ('s78',78)) STORED AS DIRECTORIES;

-- Alter Table Set Skewed Location
ALTER TABLE table_name SET SKEWED LOCATION (('value1', 'value2') = 'hdfs://location1',('value3', 'value4') = 'hdfs://location2');

-- Alter Table Constraints
ALTER TABLE table_name ADD CONSTRAINT pk_constraint PRIMARY KEY (id) DISABLE NOVALIDATE;

-- Add Partition
ALTER TABLE table_name ADD IF NOT EXISTS PARTITION (year=2023, month=12) LOCATION '/path/to/partition';

-- Rename Partition
ALTER TABLE table_name PARTITION (year=2023, month=11)
    RENAME TO PARTITION (year=2023, month=12);

-- Exchange Partition
ALTER TABLE target_table EXCHANGE PARTITION (year=2023, month=12)
    WITH TABLE source_table;

-- Recover Partitions
MSCK REPAIR TABLE table_name;

-- Drop Partition
ALTER TABLE table_name DROP IF EXISTS PARTITION (year=2023, month=12) PURGE;

-- Archive Partition
ALTER TABLE table_name ARCHIVE PARTITION (year=2023, month=12);

-- Unarchive Partition
ALTER TABLE table_name UNARCHIVE PARTITION (year=2023, month=12);

-- Alter Table/Partition File Format
ALTER TABLE table_name PARTITION (year=2023, month=12)
    SET FILEFORMAT PARQUET;

-- Alter Table/Partition Location
ALTER TABLE table_name PARTITION (year=2023, month=12)
    SET LOCATION "hdfs://new_location";

-- Alter Table/Partition Touch
ALTER TABLE table_name TOUCH PARTITION (year=2023, month=12);

-- Alter Table/Partition Compact
ALTER TABLE table_name PARTITION (year=2023, month=12)
    COMPACT 'MAJOR' AND WAIT;

-- Alter Table/Partition Concatenate
ALTER TABLE table_name PARTITION (year=2023, month=12) CONCATENATE;

-- Alter Table/Partition Update Columns
ALTER TABLE table_name PARTITION (year=2023, month=12) UPDATE COLUMNS;

-- Change Column
ALTER TABLE table_name CHANGE COLUMN old_col_name new_col_name INT
    COMMENT 'New column comment' AFTER existing_column CASCADE;

-- Add Columns
ALTER TABLE table_name ADD COLUMNS (
  new_col1 STRING COMMENT 'New column 1',
  new_col2 INT COMMENT 'New column 2'
) CASCADE;

-- Replace Columns
ALTER TABLE table_name REPLACE COLUMNS (
    col1 INT COMMENT 'Replaced column 1',
    col2 STRING COMMENT 'Replaced column 2'
    ) CASCADE;

-- Partial Partition Specification
ALTER TABLE foo PARTITION (ds='2008-04-08', hr=11) CHANGE COLUMN dec_column_name dec_column_name DECIMAL(38,18);
ALTER TABLE foo PARTITION (ds='2008-04-08', hr) CHANGE COLUMN dec_column_name dec_column_name DECIMAL(38,18);
