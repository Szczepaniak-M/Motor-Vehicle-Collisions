DROP TABLE collisions_ext;
DROP TABLE zipcode_ext;
DROP TABLE manhattan_street;

CREATE EXTERNAL TABLE IF NOT EXISTS collisions_ext(
    street STRING,
    zipcode STRING,
    person_type STRING,
    damage_type STRING,
    count INT)
COMMENT 'collisions'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS SEQUENCEFILE
LOCATION '/user/motor_vehicle_collisions/output_mr';

CREATE EXTERNAL TABLE IF NOT EXISTS zipcode_ext(
    zipcode STRING,
    borough STRING)
COMMENT 'zipcode'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/motor_vehicle_collisions/input/datasource_zip_codes'
TBLPROPERTIES("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS manhattan_street_ext(
    street STRING,
    person_type STRING,
    killed INT,
    injured INT)
COMMENT 'manhattan_street'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/motor_vehicle_collisions/output';

INSERT OVERWRITE TABLE manhattan_street_ext
SELECT street, person_type, killed, injured
FROM (
    SELECT street, person_type, SUM(killed) AS killed, SUM(injured) AS injured,
        RANK () OVER (PARTITION BY person_type ORDER BY SUM(killed) + SUM(injured) DESC) AS rank_in_person_type
    FROM (SELECT zipcode, street, person_type,
                 CASE WHEN damage_type = 'INJURED' THEN count ELSE 0 END injured,
                 CASE WHEN damage_type = 'KILLED' THEN count ELSE 0 END killed
          FROM collisions_ext) c
    JOIN zipcode_ext z ON (z.zipcode = c.zipcode)
    WHERE z.borough = 'MANHATTAN'
    GROUP BY c.street, c.person_type
) r
WHERE r.rank_in_person_type <= 3;
