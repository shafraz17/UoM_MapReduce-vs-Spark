import argparse
import time

from pyspark.sql import SparkSession

queries = [
    {"carrier_delay": f"""SELECT Year, AVG(CarrierDelay) AS Avg_Carrier_Delay
            FROM delay_flights
            GROUP BY Year
            ORDER BY Year DESC"""},
    {"nas_delay": f"""SELECT Year, AVG(NASDelay) AS Avg_NAS_Delay
            FROM delay_flights
            GROUP BY Year
            ORDER BY Year DESC"""},
    {"weather_delay": f"""SELECT Year, AVG(WeatherDelay) AS Avg_Weather_Delay
            FROM delay_flights
            GROUP BY Year
            ORDER BY Year DESC"""},
    {"late_aircraft": f"""SELECT Year, AVG(LateAircraftDelay) AS Avg_Late_Aircraft_Delay
            FROM delay_flights
            GROUP BY Year
            ORDER BY Year DESC"""},
    {"security_delay": f"""SELECT Year, AVG(SecurityDelay) AS Avg_Security_Delay
            FROM delay_flights
            GROUP BY Year
            ORDER BY Year DESC"""}
]

def query_flight_delay(data_source, output_uri, iteration = 1):
    with SparkSession.builder.appName("FlightDelay").getOrCreate() as spark:
        if data_source is not None:
            flight_delay_df = spark.read.option("header", "true").csv(data_source)

        flight_delay_df.createOrReplaceTempView('delay_flights')

        for i in range(0, iteration):
            for query in queries:
                start_time = spark.sql("SELECT current_timestamp() AS start_time")
                result = spark.sql(next(iter(query.values())))
                end_time = spark.sql("SELECT current_timestamp() AS end_time")

                query_name = next(iter(query))
                output_path = f"{output_uri}/{query_name}/{i}"
                results_output_path = f"{output_path}/QueryResults"
                query_time_ouput_path = f"{output_path}/QueryTime"

                result.write.option("header", "true").mode("overwrite").csv(results_output_path)
                start_time.write.option("header", "true").mode("overwrite").csv(f"{query_time_ouput_path}/start_time")
                end_time.write.option("header", "true").mode("overwrite").csv(f"{query_time_ouput_path}/end_time")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for CSV data, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    query_flight_delay(args.data_source, args.output_uri, 5)