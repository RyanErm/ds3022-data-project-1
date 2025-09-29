import duckdb
import logging
#using dbt

## ADD BACK IN THE ADD trip_co2_kgs column

#set up logger
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='transform.log'
)
logger = logging.getLogger(__name__)

def transform_data():
    con = None

    try:
        #Connect to local Duckdb
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        print("Connected")

        #adding row id

        #update yellow table with new data by making a copy, deleting old one, and renaming new one
        con.execute(f""" 
            CREATE TABLE yellow_trip_data_new
            AS SELECT * FROM nyc_taxi_emissions_yellow;
                    
            DROP TABLE yellow_trip_data;
            
            ALTER TABLE yellow_trip_data_new RENAME TO yellow_trip_data;
        """)
        logger.info("Updated yellow table with new data by making a copy, deleting old one, and renaming new one")
        print("Updated yellow table with new data by making a copy, deleting old one, and renaming new one")        

        #green time


        #update green table with new data by making a copy, deleting old one, and renaming new one
        con.execute(f""" 
            CREATE TABLE green_trip_data_new
            AS SELECT * FROM nyc_taxi_emissions_green;
                    
            DROP TABLE green_trip_data;
            
            ALTER TABLE green_trip_data_new RENAME TO green_trip_data;
        """)
        logger.info("Updated green table with new data by making a copy, deleting old one, and renaming new one")
        print("Updated green table with new data by making a copy, deleting old one, and renaming new one")

        
        #error handling
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

#run code
if __name__ == "__main__":
    transform_data()