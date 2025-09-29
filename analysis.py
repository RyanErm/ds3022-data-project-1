import duckdb
import logging
import matplotlib.pyplot as plt

#set up logger
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log'
)
logger = logging.getLogger(__name__)

def analyze_data():
    con = None
    try:

        #Connect to local Duckdb
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        #loop through every year
        for year in range (15,17):
            year_str = year

            #Highest emission for yellow 
            highest_carbon_y = con.execute(f""" 
                SELECT MAX(trip_co2_kgs) 
                AS highest_emission 
                FROM yellow_trip_data 
                WHERE date_part('year', tpep_pickup_datetime) = 20{year_str};
            """).fetchone()[0] #get value
            print(f"The yellow taxi trip that produced the most kilograms of CO2 in the year 20{year_str} released {highest_carbon_y} kilograms. ")
            logger.info(f"The yellow taxi trip that produced the most kilograms of CO2 in the year 20{year_str} released {highest_carbon_y} kilograms. ")

            #Highest emission for green
            highest_carbon_g = con.execute(f""" 
                SELECT MAX(trip_co2_kgs) 
                AS highest_emission 
                FROM green_trip_data 
                WHERE date_part('year', lpep_pickup_datetime) = 20{year_str};
            """).fetchone()[0] #get value
            print(f"The green taxi trip that produced the most kilograms of CO2 in the year 20{year_str} released {highest_carbon_g} kilograms. ")
            logger.info(f"The green taxi trip that produced the most kilograms of CO2 in the year 20{year_str} released {highest_carbon_g} kilograms. ")

            #Average hour for highest/lowest emission for yellow
            carbon_hour_y = con.execute(f""" 
                SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_co2
                FROM yellow_trip_data
                WHERE date_part('year', tpep_pickup_datetime) = 20{year_str}
                GROUP BY hour_of_day
                ORDER by avg_co2 DESC;
            """).fetchall() #get list in descending order
            #get first hour (the highest average)
            highest_carbon_hour_y = str(carbon_hour_y[0][0])
            #get last hour (the lowest average)
            lowest_carbon_hour_y = str(carbon_hour_y[-1][0])
            print(f"The hour of the day with the highest carbon output, on average, for yellow taxis in the year 20{year_str} was {highest_carbon_hour_y}")
            print(f"The hour of the day with the lowest carbon output, on average, for yellow taxis in the year 20{year_str} was {lowest_carbon_hour_y}")
            logger.info(f"The hour of the day with the highest carbon output, on average, for yellow taxis in the year 20{year_str} was {highest_carbon_hour_y}")
            logger.info(f"The hour of the day with the lowest carbon output, on average, for yellow taxis in the year 20{year_str} was {lowest_carbon_hour_y}")
            

            #Average hour for highest/lowest emission for green
            carbon_hour_g = con.execute(f""" 
                SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_co2
                FROM green_trip_data
                WHERE date_part('year', lpep_pickup_datetime) = 20{year_str}
                GROUP BY hour_of_day
                ORDER by avg_co2 DESC;
            """).fetchall() #get hours in descending order
            #get first hour (the highest average)
            highest_carbon_hour_g = str(carbon_hour_g[0][0])
            #get last hour (the lowest average)
            lowest_carbon_hour_g = str(carbon_hour_g[-1][0])
            print(f"The hour of the day with the highest carbon output, on average, for green taxis in the year 20{year_str} was {highest_carbon_hour_g}")
            print(f"The hour of the day with the lowest carbon output, on average, for green taxis in the year 20{year_str} was {lowest_carbon_hour_g}")
            logger.info(f"The hour of the day with the highest carbon output, on average, for green taxis in the year 20{year_str} was {highest_carbon_hour_g}")
            logger.info(f"The hour of the day with the lowest carbon output, on average, for green taxis in the year 20{year_str} was {lowest_carbon_hour_g}")
            

            #Average day with highest/lowest emission for yellow
            carbon_day_y = con.execute(f""" 
                SELECT day_of_week, AVG(trip_co2_kgs) AS avg_co2
                FROM yellow_trip_data
                WHERE date_part('year', tpep_pickup_datetime) = 20{year_str}
                GROUP BY day_of_week
                ORDER by avg_co2 DESC;
            """).fetchall() #get days in descending order
            #get first day (the highest average)
            highest_carbon_day_y = str(carbon_day_y[0][0])
            #get last day (the lowest average)
            lowest_carbon_day_y = str(carbon_day_y[-1][0])
            print(f"The day of the week with the highest carbon output, on average, for yellow taxis in the year 20{year_str} was {highest_carbon_day_y}")
            print(f"The day of the week with the lowest carbon output, on average, for yellow taxis in the year 20{year_str} was {lowest_carbon_day_y}")
            logger.info(f"The day of the week with the highest carbon output, on average, for yellow taxis in the year 20{year_str} was {highest_carbon_day_y}")
            logger.info(f"The day of the week with the lowest carbon output, on average, for yellow taxis in the year 20{year_str} was {lowest_carbon_day_y}")
            

            #Average day with highest/lowest emission for green
            carbon_day_g = con.execute(f""" 
                SELECT day_of_week, AVG(trip_co2_kgs) AS avg_co2
                FROM green_trip_data
                WHERE date_part('year', lpep_pickup_datetime) = 20{year_str}
                GROUP BY day_of_week
                ORDER by avg_co2 DESC;
            """).fetchall()#get days in descending order
            #get first day (the highest average)
            highest_carbon_day_g = str(carbon_day_g[0][0])
            #get last day (the lowest average)
            lowest_carbon_day_g = str(carbon_day_g[-1][0])
            print(f"The day of the week with the highest carbon output, on average, for green taxis in the year 20{year_str} was {highest_carbon_day_g}")
            print(f"The day of the week with the lowest carbon output, on average, for green taxis in the year 20{year_str} was {lowest_carbon_day_g}")
            logger.info(f"The day of the week with the highest carbon output, on average, for green taxis in the year 20{year_str} was {highest_carbon_day_g}")
            logger.info(f"The day of the week with the lowest carbon output, on average, for green taxis in the year 20{year_str} was {lowest_carbon_day_g}")
            

            #Average week with highest/lowest emission for yellow
            carbon_week_y = con.execute(f""" 
                SELECT week_of_year, AVG(trip_co2_kgs) AS avg_co2
                FROM yellow_trip_data
                WHERE date_part('year', tpep_pickup_datetime) = 20{year_str}
                GROUP BY week_of_year
                ORDER by avg_co2 DESC;
            """).fetchall() #get weeks in descending order
            #get the first week (the highest average)
            highest_carbon_week_y = str(carbon_week_y[0][0])
            #get the last week (the lowest average)
            lowest_carbon_week_y = str(carbon_week_y[-1][0])
            print(f"The week of the year with the highest carbon output, on average, for yellow taxis in the year 20{year_str} was {highest_carbon_week_y}")
            print(f"The week of the year with the lowest carbon output, on average, for yellow taxis in the year 20{year_str} was {lowest_carbon_week_y}")
            logger.info(f"The week of the year with the highest carbon output, on average, for yellow taxis in the year 20{year_str} was {highest_carbon_week_y}")
            logger.info(f"The week of the year with the lowest carbon output, on average, for yellow taxis in the year 20{year_str} was {lowest_carbon_week_y}")
            

            #Average week with highest/lowest emission for green
            carbon_week_g = con.execute(f""" 
                SELECT week_of_year, AVG(trip_co2_kgs) AS avg_co2
                FROM green_trip_data
                WHERE date_part('year', lpep_pickup_datetime) = 20{year_str}
                GROUP BY week_of_year
                ORDER by avg_co2 DESC;
            """).fetchall() #get weeks in descending order
            #get the first week (the highest average)
            highest_carbon_week_g = str(carbon_week_g[0][0])
            #get the last week (the lowest average)
            lowest_carbon_week_g = str(carbon_week_g[-1][0])
            print(f"The week of the year with the highest carbon output, on average, for green taxis in the year 20{year_str} was {highest_carbon_week_g}")
            print(f"The week of the year with the lowest carbon output, on average, for green taxis in the year 20{year_str} was {lowest_carbon_week_g}")
            logger.info(f"The week of the year with the highest carbon output, on average, for green taxis in the year 20{year_str} was {highest_carbon_week_g}")
            logger.info(f"The week of the year with the lowest carbon output, on average, for green taxis in the year 20{year_str} was {lowest_carbon_week_g}")
            
        
            #Average month with highest/lowest emission for yellow
            carbon_month_y = con.execute(f""" 
                SELECT month_of_year, AVG(trip_co2_kgs) AS avg_co2
                FROM yellow_trip_data
                WHERE date_part('year', tpep_pickup_datetime) = 20{year_str}
                GROUP BY month_of_year
                ORDER by avg_co2 DESC;
            """).fetchall() #get list of months in descending order

            #get first month (the highest average)
            highest_carbon_month_y = str(carbon_month_y[0][0])
            #get last month (the lowest average)
            lowest_carbon_month_y = str(carbon_month_y[-1][0])
            print(f"The month of the year with the highest carbon output, on average, for yellow taxis in the year 20{year_str} was {highest_carbon_month_y}")
            print(f"The month of the year with the lowest carbon output, on average, for yellow taxis in the year 20{year_str} was {lowest_carbon_month_y}")
            logger.info(f"The month of the year with the highest carbon output, on average, for yellow taxis in the year 20{year_str} was {highest_carbon_month_y}")
            logger.info(f"The month of the year with the lowest carbon output, on average, for yellow taxis in the year 20{year_str} was {lowest_carbon_month_y}")
            

            #Average month with highest/lowest emission for green
            carbon_month_g = con.execute(f""" 
                SELECT month_of_year, AVG(trip_co2_kgs) AS avg_co2
                FROM green_trip_data
                WHERE date_part('year', lpep_pickup_datetime) = 20{year_str}
                GROUP BY month_of_year
                ORDER by avg_co2 DESC;
            """).fetchall() #get months in descending order

            #get first month (the highest average)
            highest_carbon_month_g = str(carbon_month_g[0][0])
            #get last month (the lowest average)
            lowest_carbon_month_g = str(carbon_month_g[-1][0])
            print(f"The month of the year with the highest carbon output, on average, for green taxis in the year 20{year_str} was {highest_carbon_month_g}")
            print(f"The month of the year with the lowest carbon output, on average, for green taxis in the year 20{year_str} was {lowest_carbon_month_g}")
            logger.info(f"The month of the year with the highest carbon output, on average, for green taxis in the year 20{year_str} was {highest_carbon_month_g}")
            logger.info(f"The month of the year with the lowest carbon output, on average, for green taxis in the year 20{year_str} was {lowest_carbon_month_g}")
            

        #Make the plot

        #get averages by month and order by month and year
        yellow_data = con.execute(f""" 
            SELECT date_part('year', tpep_pickup_datetime) AS year, 
                SUM(trip_co2_kgs) AS month_sum, 
                CASE month_of_year
                    WHEN 'January' THEN 1
                    WHEN 'February' THEN 2
                    WHEN 'March' THEN 3
                    WHEN 'April' THEN 4
                    WHEN 'May' THEN 5
                    WHEN 'June' THEN 6
                    WHEN 'July' THEN 7
                    WHEN 'August' THEN 8
                    WHEN 'September' THEN 9
                    WHEN 'October' THEN 10
                    WHEN 'November' THEN 11
                    WHEN 'December' THEN 12
                END AS month_of_year
            FROM yellow_trip_data
            GROUP BY year, month_of_year
            ORDER BY year, month_of_year
        """).fetchall()
        print(yellow_data)

        logger.info("Collecting yellow data for image")
        #flatten the data
        flat_yellow_data = []
        for year in range (len(yellow_data)):
            for month in range (yellow_data[year]):
                dat_str = yellow_data[year][month]
                flat_yellow_data = dat_str

        #get averages by month and order by month and year
        green_data = con.execute(f""" 
            SELECT date_part('year', lpep_pickup_datetime) AS year, 
                SUM(trip_co2_kgs) AS month_sum,
                CASE month_of_year
                    WHEN 'January' THEN 1
                    WHEN 'February' THEN 2
                    WHEN 'March' THEN 3
                    WHEN 'April' THEN 4
                    WHEN 'May' THEN 5
                    WHEN 'June' THEN 6
                    WHEN 'July' THEN 7
                    WHEN 'August' THEN 8
                    WHEN 'September' THEN 9
                    WHEN 'October' THEN 10
                    WHEN 'November' THEN 11
                    WHEN 'December' THEN 12
                END AS month_of_year
            FROM green_trip_data
            GROUP BY year, month_of_year
            ORDER BY year, month_of_year
        """).fetchall()
        print(green_data.count)

        logger.info("Collecting green data for image")

        #flatten the data
        flat_green_data = []
        for year in range(len(green_data)):
            for month in range (len(year)):
                dat_str = green_data[year][month]
                flat_green_data = dat_str

        logger.info("Creating histogram...")
        # Create histogram
        plt.hist([flat_yellow_data, flat_green_data], label=['Yellow Taxi', 'Green Taxi'], color=['yellow', 'green'])

        # Add labels and legend
        plt.xlabel('Month')
        plt.ylabel('Average Kg of CO2 per month')
        plt.title('Histogram of CO2 output per month from 2015-2024 of Yellow and Green Taxi Cabs')
        plt.legend()

        # Show the plot
        plt.savefig('Taxi.png')
        plt.show()
            

        logger.info("Done!")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    analyze_data()
