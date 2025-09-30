import duckdb
import os
import logging
import time

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def load_parquet_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        print("CONNECTED")

        #Delete the old emissions table
        con.execute(F"""
                DROP TABLE IF EXISTS emissions_data;
            """)
        logger.info("Dropped emissions table if exists")
        #Create the emissions table
        con.execute(F"""
                CREATE TABLE emissions_data AS
                SELECT * FROM read_csv_auto("../data/vehicle_emissions.csv");
            """)
        logger.info("Created emissions table")

        #dont't recreate the table
        con.execute(f"""
                DROP TABLE IF EXISTS yellow_trip_data;
             """)
        logger.info("Dropped yellow table if exists")

        #create the table
        con.execute(f"""
                -- SQL goes here
                CREATE TABLE yellow_trip_data(
                    tpep_pickup_datetime TIMESTAMP, 
                    tpep_dropoff_datetime TIMESTAMP, 
                    trip_distance DOUBLE, 
                    passenger_count BIGINT)
            """)
        logger.info("Created yellow trip data table")


        #dont't recreate the table
        con.execute(f"""
                DROP TABLE IF EXISTS green_trip_data;
             """)
        logger.info("Dropped green table if exists")

        #create the table
        con.execute(f"""
                -- SQL goes here
                CREATE TABLE green_trip_data(
                    lpep_pickup_datetime TIMESTAMP, 
                    lpep_dropoff_datetime TIMESTAMP, 
                    trip_distance DOUBLE, 
                    passenger_count BIGINT)
            """)
        logger.info("Created green trip data table")

        #collecting data for each year
        for year in range(15,25): #year for loop
            for month in range (1,13): #month for loop
                year_str = str(year) #converting to string
                if month<10: #convert the month number to the proper format
                    month_str = str(month) #convert to string
                    month_str = "0" + month_str
                else:
                    month_str = str(month) #convert to string
                
                #make new input files for each year, month, and taxi type
                input_file_yellow = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_20{year_str}-{month_str}.parquet"
                input_file_green = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_20{year_str}-{month_str}.parquet"
                
                #insert yellow trip data
                con.execute(f"""
                        INSERT INTO yellow_trip_data(tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, passenger_count)
                        SELECT tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, passenger_count FROM read_parquet("{input_file_yellow}");
             """)
                logger.info(f"Inserted the following data into yellow table: Month - {month_str}, Year-20{year_str}  ")
                print(f"Inserted the following data into yellow table: Month - {month_str}, Year-20{year_str}  ")
                time.sleep(40)
                
                #insert green trip data
                con.execute(f"""
                        INSERT INTO green_trip_data (lpep_pickup_datetime, lpep_dropoff_datetime, trip_distance, passenger_count)
                        SELECT lpep_pickup_datetime, lpep_dropoff_datetime, trip_distance, passenger_count FROM read_parquet("{input_file_green}");
                """)
                logger.info(f"Inserted the following data into green table: Month - {month_str}, Year-20{year_str}  ")
                print(f"Inserted the following data into green table: Month - {month_str}, Year-20{year_str}  ")
                time.sleep(40)

        num_yellow = con.execute(f""" 
            SELECT COUNT(*) FROM yellow_trip_data;
        """).fetchone()[0]
        num_yellow = str(num_yellow)
        logger.info(f"The initial number of entries in the yellow table, before cleaning, is {num_yellow}")
        print(f"The initial number of entries in the yellow table, before cleaning, is {num_yellow}")

        num_green = con.execute(f""" 
            SELECT COUNT(*) FROM green_trip_data;
        """).fetchone()[0]
        num_green = str(num_green)
        logger.info(f"The initial number of entries in the green table, before cleaning, is {num_green}")
        print(f"The initial number of entries in the green table, before cleaning, is {num_green}")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()

