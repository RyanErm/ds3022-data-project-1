import duckdb
import logging
import matplotlib.pyplot as plt

#set up logger
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log'
)
logger = logging.getLogger(__name__)

#main function
def analyze_data():
    con = None
    try:

        #Connect to local Duckdb
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        print("Connected to Duckdb instance")

        #loop through every year
        for year in range (15,25):
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

        #get sums by month and order by month and year
        logger.info("Collecting yellow data for image")
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
        """).fetchall() #get data
        #yellow_data is a list of tuples that have the following info: (Year, aggregated emissions per month, month)

        #flatten the data
        flat_yellow_data = {}
        #loop through each month
        for num in range(len(yellow_data)):
            #create a new id for the month based on the month and year
            year_lab = yellow_data[num][2] * (yellow_data[num][0] - 2014)
            #add to dictionairy
            flat_yellow_data[year_lab] = yellow_data[num][1]


        #get sums by month and order by month and year
        logger.info("Collecting green data for image")
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
        """).fetchall()#get data
        #yellow_data is a list of tuples that have the following info: (Year, aggregated emissions per month, month)


        #flatten the data
        flat_green_data = {}
        #go through each unique month
        for num in range(len(green_data)):
            #create a new id for each month based on the month and year
            year_lab = green_data[num][2] * (green_data[num][0] - 2014)
            #add to dictionairy
            flat_green_data[year_lab] = green_data[num][1]
        
        

        logger.info("Creating lineplot...")

        # Create lineplot
        plt.plot(flat_yellow_data.keys(), flat_yellow_data.values(), color = "yellow", marker = "o", label = "Yellow Taxis")
        plt.plot(flat_green_data.keys(), flat_green_data.values(), color = "green", marker = "o", label = "Green Taxis" )

        # Add labels and legend
        plt.xlabel('Months (Jan 2015 = 1, Jan 2016 = 13, etc)')
        plt.ylabel('Aggregate Kg of CO2 per month (scale of 1e7)')
        plt.title('Time plot of CO2 output per month from 2015-2024 of \n Yellow and Green Taxi Cabs')
        plt.tight_layout()
        plt.legend()

        # Show the plot and save it
        plt.savefig('Taxi.png')
        plt.show()
            

        logger.info("Done!")

    #error handling
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

#run function
if __name__ == "__main__":
    analyze_data()
