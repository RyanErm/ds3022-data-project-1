import duckdb
import logging

#CHANGE THE DAY, MONTH, sql statements

#set up logger
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

def clean_data():
    con = None

    try:
        #Connect to local Duckdb
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")



        ### PRE CLEANING TESTING
        print("Starting Pre cleaning testing")
        logger.info("Starting Pre cleaning testing")



        #initial count of data in yellow
        num_yellow_initial = con.execute(F""" 
            SELECT COUNT(*) FROM yellow_trip_data;
        """).fetchone()[0] #get count
        #convert to string
        num_yellow_initial = str(num_yellow_initial)
        logger.info(f"The initial amount of rows in the yellow table is: {num_yellow_initial}")
        print(f"The initial amount of rows in the yellow table is: {num_yellow_initial}")

        #initial count of data in green
        num_green_initial = con.execute(F""" 
            SELECT COUNT(*) FROM green_trip_data;
        """).fetchone()[0] #get count
        #convert to string
        num_green_initial = str(num_green_initial)
        logger.info(f"The initial amount of rows in the green table is: {num_green_initial}")
        print(f"The initial amount of rows in the green table is: {num_green_initial}")

        #test for passenger count for yellow table
        no_yellow = con.execute(f"""
            SELECT COUNT(*) FROM yellow_trip_data
            WHERE passenger_count=0;        
        """).fetchone()[0] #get count
        #convert to string
        no_yellow = str(no_yellow)
        print(f"There are {no_yellow} no passenger trips in yellow")
        logger.info(f"Tested for no passengers in yellow table. Amount left:{no_yellow} ")

        #test for passenger count for green table
        no_green = con.execute(f"""
            SELECT COUNT(*) FROM green_trip_data
            WHERE passenger_count=0;        
        """).fetchone()[0] #get count
        #convert to string
        no_green = str(no_green)
        print(f"There are {no_green} no passenger trips in green")
        logger.info(f"Tested for no passengers in green table. Amount left:{no_green} ")

        #test for no distance for yellow table
        dist_yellow = con.execute(f"""
            SELECT COUNT(*) FROM yellow_trip_data
            WHERE trip_distance=0;        
        """).fetchone()[0] #get count
        #convert to string
        dist_yellow = str(dist_yellow)
        print(f"There are {dist_yellow} no distance trips in yellow")
        logger.info(f"Tested for no distance trips in yellow table. Amount left:{dist_yellow} ")

        #test for no distance for green table
        dist_green = con.execute(f"""
            SELECT COUNT(*) FROM green_trip_data
            WHERE trip_distance=0;        
        """).fetchone()[0] #get count
        #convert to string
        dist_green = str(dist_green)
        print(f"There are {dist_green} no distance trips in green")
        logger.info(f"Tested for no distance trips in green table. Amount left:{dist_green} ")

        #test for long for yellow table
        long_yellow = con.execute(f"""
            SELECT COUNT(*) FROM yellow_trip_data
            WHERE trip_distance>100;        
        """).fetchone()[0] #get count
        #convert to string
        long_yellow = str(long_yellow)
        print(f"There are {long_yellow} long distance trips in yellow")
        logger.info(f"Tested for long distance trips in yellow table. Amount left:{long_yellow} ")

        #test for long distance for green table
        long_green = con.execute(f"""
            SELECT COUNT(*) FROM green_trip_data
            WHERE trip_distance>100;        
        """).fetchone()[0] #get count
        #convert to string
        long_green = str(long_green)
        print(f"There are {long_green} long distance trips in green")
        logger.info(f"Tested for long distance trips in green table. Amount left:{long_green} ")

        #test for trips over 1 day for yellow table
        day_yellow = con.execute(f"""
            SELECT COUNT(*) FROM yellow_trip_data
            WHERE date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) > 86400;         
        """).fetchone()[0] #get count
        #convert to string
        day_yellow = str(day_yellow)
        print(f"There are {day_yellow} day trips in yellow")
        logger.info(f"Tested for day trips in yellow table. Amount left:{day_yellow} ")

        #test for trips over 1 day for green table
        day_green = con.execute(f"""
            SELECT COUNT(*) FROM green_trip_data
            WHERE date_diff('second', lpep_pickup_datetime, lpep_dropoff_datetime) > 86400;        
        """).fetchone()[0] #get count
        #convert to string
        day_green = str(day_green)
        print(f"There are {day_green} day trips in green")
        logger.info(f"Tested for day trips in green table. Amount left:{day_green} ")

        #test for duplicates for yellow table
        #get table of unique values
        con.execute(f"""
            CREATE TABLE yellow_trip_data_clean_test_pre AS 
            SELECT DISTINCT * FROM yellow_trip_data;     
        """)
        #get count of unique entries
        yellow_num_pre = con.execute(f""" 
            SELECT COUNT(*) FROM yellow_trip_data_clean_test_pre
        """).fetchone()[0] #get count
        #get count of unique and non unique entries
        yellow_dup_num_pre = con.execute(f""" 
            SELECT COUNT(*) FROM yellow_trip_data;
        """).fetchone()[0] #get count
        #delete unique table
        con.execute(f""" 
            DROP TABLE yellow_trip_data_clean_test_pre
        """)
        #ensure that these numbers are the same (difference should be zero)
        dup_yellow_pre = yellow_dup_num_pre-yellow_num_pre
        #convert to string
        dup_yellow_pre = str(dup_yellow_pre)
        print(f"There are {dup_yellow_pre} duplicate trips in yellow")
        logger.info(f"Tested for duplicate trips in yellow table. Amount left:{dup_yellow_pre} ")

        #test for duplicates for green table
        con.execute(f"""
            CREATE TABLE green_trip_data_clean_test_pre AS 
            SELECT DISTINCT * FROM green_trip_data;     
        """)
        #get count of unique entries
        green_num_pre = con.execute(f""" 
            SELECT COUNT(*) FROM green_trip_data_clean_test_pre
        """).fetchone()[0] #get count
        #get count of unique and non unique entries
        green_dup_num_pre = con.execute(f""" 
            SELECT COUNT(*) FROM green_trip_data;
        """).fetchone()[0] #get count
        #delete unique table
        con.execute(f""" 
            DROP TABLE green_trip_data_clean_test_pre
        """)
        #ensure that these numbers are the same (difference should be zero)
        dup_green_pre = green_dup_num_pre-green_num_pre
        #convert to string
        dup_green_pre = str(dup_green_pre)
        print(f"There are {dup_green_pre} duplicate trips in green")
        logger.info(f"Tested for duplicate trips in green table. Amount left:{dup_green_pre} ")



        print("Finished pre cleaning testing")
        logger.info("Finished pre cleaning testing")
        ### START CLEANING DATA
        print("Starting to clean data")
        logger.info("Starting to clean data")



        #delete duplicate entries for yellow table
        con.execute(f"""
            CREATE TABLE yellow_trip_data_clean AS 
            SELECT DISTINCT * FROM yellow_trip_data;

            DROP TABLE yellow_trip_data;
            ALTER TABLE yellow_trip_data_clean RENAME TO yellow_trip_data;
        """)
        logger.info("Removed Duplicate Entries in yellow table")

        #delete duplicate entries for green table
        con.execute(f"""
            CREATE TABLE green_trip_data_clean AS 
            SELECT DISTINCT * FROM green_trip_data;

            DROP TABLE green_trip_data;
            ALTER TABLE green_trip_data_clean RENAME TO green_trip_data;
        """)
        logger.info("Removed Duplicate Entries in green table")

        #delete entries with no passengers for yellow table
        con.execute(f"""
            DELETE FROM yellow_trip_data
            WHERE passenger_count = 0;       
        """)
        logger.info("Removed entries where there were no passengers from yellow table")

        #delete entries with no passengers for green table
        con.execute(f"""
            DELETE FROM green_trip_data
            WHERE passenger_count = 0;       
        """)
        logger.info("Removed entries where there were no passengers from green table")

        #delete entries that had no distance for yellow table
        con.execute(f"""
            DELETE FROM yellow_trip_data
            WHERE trip_distance = 0;       
        """)
        logger.info("Removed entries that had no distance from yellow table")

        #delete entries that had no distance for green table
        con.execute(f"""
            DELETE FROM green_trip_data
            WHERE trip_distance = 0;       
        """)
        logger.info("Removed entries that had no distance from green table")

        #delete entries longer than 100 miles for yellow table
        con.execute(f"""
            DELETE FROM yellow_trip_data
            WHERE trip_distance > 100;       
        """)
        logger.info("Removed entries that were longer than 100 miles from yellow table")

        #delete entries longer than 100 miles for green table
        con.execute(f"""
            DELETE FROM green_trip_data
            WHERE trip_distance > 100;       
        """)
        logger.info("Removed entries that were longer than 100 miles from green table")

        #delete entries longer than 1 day for yellow table
        con.execute(f"""
            DELETE FROM yellow_trip_data
            WHERE date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) > 86400;       
        """)
        logger.info("Removed entries that were longer than 1 day from yellow table")


        #delete entries longer than 1 day for green table
        con.execute(f"""
            DELETE FROM green_trip_data
            WHERE date_diff('second', lpep_pickup_datetime, lpep_dropoff_datetime) > 86400;        
        """)
        logger.info("Removed entries that were longer than 1 day from green table")



        print("Finished cleaning")
        logger.info("Finished cleaning")
        #POST CLEANING TESTING
        print("Starting Post Cleaning Testing")
        logger.info("Starting post cleaning testing")



        #test for passenger count for yellow table
        no_yellow = con.execute(f"""
            SELECT COUNT(*) FROM yellow_trip_data
            WHERE passenger_count=0;        
        """).fetchone()[0] #get count
        #convert to string
        no_yellow = str(no_yellow)
        print(f"There are {no_yellow} no passenger trips in yellow")
        logger.info(f"Tested for no passengers in yellow table. Amount left:{no_yellow} ")

        #test for passenger count for green table
        no_green = con.execute(f"""
            SELECT COUNT(*) FROM green_trip_data
            WHERE passenger_count=0;        
        """).fetchone()[0] #get count
        #convert to string
        no_green = str(no_green)
        print(f"There are {no_green} no passenger trips in green")
        logger.info(f"Tested for no passengers in green table. Amount left:{no_green} ")


        #test for no distance for yellow table
        dist_yellow = con.execute(f"""
            SELECT COUNT(*) FROM yellow_trip_data
            WHERE trip_distance=0;        
        """).fetchone()[0] #get count
        #convert to string
        dist_yellow = str(dist_yellow)
        print(f"There are {dist_yellow} no distance trips in yellow")
        logger.info(f"Tested for no distance trips in yellow table. Amount left:{dist_yellow} ")

        #test for no distance for green table
        dist_green = con.execute(f"""
            SELECT COUNT(*) FROM green_trip_data
            WHERE trip_distance=0;        
        """).fetchone()[0] #get count
        #convert to string
        dist_green = str(dist_green)
        print(f"There are {dist_green} no distance trips in green")
        logger.info(f"Tested for no distance trips in green table. Amount left:{dist_green} ")

        #test for long for yellow table
        long_yellow = con.execute(f"""
            SELECT COUNT(*) FROM yellow_trip_data
            WHERE trip_distance>100;        
        """).fetchone()[0] #get count
        #convert to string
        long_yellow = str(long_yellow)
        print(f"There are {long_yellow} long distance trips in yellow")
        logger.info(f"Tested for long distance trips in yellow table. Amount left:{long_yellow} ")

        #test for long distance for green table
        long_green = con.execute(f"""
            SELECT COUNT(*) FROM green_trip_data
            WHERE trip_distance>100;        
        """).fetchone()[0] #get count
        #convert to string
        long_green = str(long_green)
        print(f"There are {long_green} long distance trips in green")
        logger.info(f"Tested for long distance trips in green table. Amount left:{long_green} ")

        #test for trips over 1 day for yellow table
        day_yellow = con.execute(f"""
            SELECT COUNT(*) FROM yellow_trip_data
            WHERE date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) > 86400;         
        """).fetchone()[0] #get count
        #convert to string
        day_yellow = str(day_yellow)
        print(f"There are {day_yellow} day trips in yellow")
        logger.info(f"Tested for day trips in yellow table. Amount left:{day_yellow} ")

        #test for trips over 1 day for green table
        day_green = con.execute(f"""
            SELECT COUNT(*) FROM green_trip_data
            WHERE date_diff('second', lpep_pickup_datetime, lpep_dropoff_datetime) > 86400;        
        """).fetchone()[0] #get count
        #convert to string
        day_green = str(day_green)
        print(f"There are {day_green} day trips in green")
        logger.info(f"Tested for day trips in green table. Amount left:{day_green} ")

        #test for duplicates for yellow table
        #get table of unique values
        con.execute(f"""
            CREATE TABLE yellow_trip_data_clean_test_post AS 
            SELECT DISTINCT * FROM yellow_trip_data;     
        """)
        #get count of unique entries
        yellow_num = con.execute(f""" 
            SELECT COUNT(*) FROM yellow_trip_data_clean_test_post
        """).fetchone()[0] #get count
        #get count of unique and non unique entries
        yellow_dup_num = con.execute(f""" 
            SELECT COUNT(*) FROM yellow_trip_data;
        """).fetchone()[0] #get count
        #delete unique table
        con.execute(f""" 
            DROP TABLE yellow_trip_data_clean_test_post
        """)
        #ensure that these numbers are the same (difference should be zero)
        dup_yellow = yellow_dup_num-yellow_num
        #convert to string
        dup_yellow = str(dup_yellow)
        print(f"There are {dup_yellow} duplicate trips in yellow")
        logger.info(f"Tested for duplicate trips in yellow table. Amount left:{dup_yellow} ")

        #test for duplicates for green table
        con.execute(f"""
            CREATE TABLE green_trip_data_clean_test AS 
            SELECT DISTINCT * FROM green_trip_data;     
        """)
        #get count of unique entries
        green_num = con.execute(f""" 
            SELECT COUNT(*) FROM green_trip_data_clean_test
        """).fetchone()[0] #get count
        #get count of unique and non unique entries
        green_dup_num = con.execute(f""" 
            SELECT COUNT(*) FROM green_trip_data;
        """).fetchone()[0] #get count
        #delete unique table
        con.execute(f""" 
            DROP TABLE green_trip_data_clean_test
        """)
        #ensure that these numbers are the same (difference should be zero)
        dup_green = green_dup_num-green_num
        #convert to string
        dup_green = str(dup_green)
        print(f"There are {dup_green} duplicate trips in green")
        logger.info(f"Tested for duplicate trips in green table. Amount left:{dup_green} ")

        #final count of data in yellow
        num_yellow_final = con.execute(F""" 
            SELECT COUNT(*) FROM yellow_trip_data;
        """).fetchone()[0] #get count
        #convert to string
        num_yellow_final = str(num_yellow_final)
        logger.info(f"The final amount of rows in the yellow table is: {num_yellow_final}")
        print(f"The final amount of rows in the yellow table is: {num_yellow_final}")

        #final count of data in green
        num_green_final = con.execute(F""" 
            SELECT COUNT(*) FROM green_trip_data;
        """).fetchone()[0] #get count
        #convert to string
        num_green_final = str(num_green_final)
        logger.info(f"The final amount of rows in the green table is: {num_green_final}")
        print(f"The final amount of rows in the green table is: {num_green_final}")


        print("Finished Post cleaning testing")
        logger.info("Finished Post cleaning testing")



    #error handling
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

#run code
if __name__ == "__main__":
    clean_data()