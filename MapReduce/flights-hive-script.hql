-- Create Table Schema with columns
CREATE EXTERNAL TABLE IF NOT EXISTS delay_flights (
    No INT,
    Year INT,
    Month INT,
    DayofMonth INT,
    DayOfWeek INT,
    DepTime INT,
    CRSDepTime INT,
    ArrTime INT,
    CRSArrTime INT,
    UniqueCarrier STRING,
    FlightNum STRING,
    TailNum STRING,
    ActualElapsedTime INT,
    CRSElapsedTime INT,
    AirTime INT,
    ArrDelay INT,
    DepDelay INT,
    Origin STRING,
    Dest STRING,
    Distance INT,
    TaxiIn INT,
    TaxiOut INT,
    Cancelled BOOLEAN,
    CancellationCode STRING,
    Diverted BOOLEAN,
    CarrierDelay INT,
    NASDelay INT,
    WeatherDelay INT,
    LateAircraftDelay INT,
    SecurityDelay INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA INPATH '${INPUT}' INTO TABLE delay_flights;

INSERT OVERWRITE DIRECTORY '${OUTPUT}/carrier_delay_query/${ITERATION}/timestamps/start_time'
    SELECT unix_timestamp(current_timestamp()) as start_time;
SET hivevar:carrier_delay_query_results = SELECT Year, AVG(CarrierDelay) AS Avg_Carrier_Delay FROM delay_flights GROUP BY Year ORDER BY Year DESC;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/carrier_delay_query/${ITERATION}/timestamps/end_time'
    SELECT unix_timestamp(current_timestamp()) as end_time;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/carrier_delay_query/${ITERATION}/results'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    ${hivevar:carrier_delay_query_results};

INSERT OVERWRITE DIRECTORY '${OUTPUT}/nas_delay_query/${ITERATION}/timestamps/start_time'
    SELECT unix_timestamp(current_timestamp()) as start_time;
SET hivevar:nas_delay_query_results = SELECT Year, AVG(NASDelay) AS Avg_NAS_Delay FROM delay_flights GROUP BY Year ORDER BY Year DESC;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/nas_delay_query/${ITERATION}/timestamps/end_time'
    SELECT unix_timestamp(current_timestamp()) as end_time;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/nas_delay_query/${ITERATION}/results'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    ${hivevar:nas_delay_query_results};

INSERT OVERWRITE DIRECTORY '${OUTPUT}/weather_delay_query/${ITERATION}/timestamps/start_time'
    SELECT unix_timestamp(current_timestamp()) as start_time;
SET hivevar:weather_delay_query_results = SELECT Year, AVG(WeatherDelay) AS Avg_Weather_Delay FROM delay_flights GROUP BY Year ORDER BY Year DESC;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/weather_delay_query/${ITERATION}/timestamps/end_time'
    SELECT unix_timestamp(current_timestamp()) as end_time;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/weather_delay_query/${ITERATION}/results'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    ${hivevar:weather_delay_query_results};

INSERT OVERWRITE DIRECTORY '${OUTPUT}/late_aircraft_delay_query/${ITERATION}/timestamps/start_time'
    SELECT unix_timestamp(current_timestamp()) as start_time;
SET hivevar:late_aircraft_delay_query_results = SELECT Year, AVG(LateAircraftDelay) AS Avg_Late_Aircraft_Delay FROM delay_flights GROUP BY Year ORDER BY Year DESC;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/late_aircraft_delay_query/${ITERATION}/timestamps/end_time'
    SELECT unix_timestamp(current_timestamp()) as end_time;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/late_aircraft_delay_query/${ITERATION}/results'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    ${hivevar:late_aircraft_delay_query_results};

INSERT OVERWRITE DIRECTORY '${OUTPUT}/security_delay_query/${ITERATION}/timestamps/start_time'
    SELECT unix_timestamp(current_timestamp()) as start_time;
SET hivevar:security_delay_query_results = SELECT Year, AVG(SecurityDelay) AS Avg_Security_Delay FROM delay_flights GROUP BY Year ORDER BY Year DESC;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/security_delay_query/${ITERATION}/timestamps/end_time'
    SELECT unix_timestamp(current_timestamp()) as end_time;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/security_delay_query/${ITERATION}/results'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    ${hivevar:security_delay_query_results};