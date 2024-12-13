from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, desc, row_number, upper, sum, dense_rank, regexp_extract
from pyspark.sql.window import Window
import sys
sys.path.insert(0, '..')
from utils.utils import load_csv_as_df, write_df_as_csv
from utils.logger import logger 

class CrashAnalysis:
    def __init__(self, spark: SparkSession, config:dict):
        """
        Initialize the CrashAnalysis class.

        Args:
            spark (SparkSession): The Spark session object.
            self.__input_config (dict): Dictionary containing input file paths.
            output_paths (dict): Dictionary containing output file paths.
        """
        self.__input_config = config['input_paths']
        self.output_path = config['output_path']
        self.spark = spark

        # Load all datasets as DataFrames
        self.charges_df = load_csv_as_df(spark,self.__input_config['charges'])
        self.endorsements_df = load_csv_as_df(spark,self.__input_config['endorsements'])
        self.restrict_df = load_csv_as_df(spark,self.__input_config['restrict'])
        self.damages_df = load_csv_as_df(spark,self.__input_config['damages'])
        self.primary_person_df = load_csv_as_df(spark,self.__input_config['primary_person'])
        self.unit_df = load_csv_as_df(spark,self.__input_config['unit'])

    def count_number_of_male_crashes(self):
        """
        Find the number of crashes where the number of males killed is greater than 2.
        """
        result = self.primary_person_df.filter((col("PRSN_GNDR_ID") == "MALE") & (col("DEATH_CNT") > 2)).count()
        df = self.spark.createDataFrame([{"NO_OF_MALE_KILLED":result}])

        logger.info(f"Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2? \nAnswer - {result}")

        write_df_as_csv(df, self.output_path+'/1')
        
        return result


    def count_two_wheeler_crashes(self):
        """
        Find the number of two-wheelers booked for crashes.
        """
        result = (self.unit_df
                  .filter(upper(col('VEH_BODY_STYL_ID')).like('%MOTORCYCLE%'))
        ).count()
        df = self.spark.createDataFrame([{"NO_OF_TWO_WHEELERS_BOOKED_FOR_CRASHES":result}])

        logger.info(f"Analysis 2: How many two wheelers are booked for crashes? \nAnswer - {result}")
            
        write_df_as_csv(df, self.output_path+'/2')
        
        return result


    def find_top5_crashes_where_driver_died_without_airbag(self):
        """
        Determine the top 5 vehicle makes of cars in crashes where the driver died and airbags did not deploy.
        """
        df = (self.unit_df.join(self.primary_person_df, on=["CRASH_ID"])
            .filter(
                (col("PRSN_INJRY_SEV_ID") == "KILLED")
                & (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED")
                & (col("VEH_MAKE_ID") != "NA")
            )
            .groupby("VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(5)).drop("count")
        result = list([vehicle[0] for vehicle in df.collect()])

        logger.info(f"Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy. \nAnswer - {result}")

        write_df_as_csv(df, self.output_path+'/3')
        
        return result


    def count_vehicles_with_drivers_having_valid_license_with_hitandrun(self):
        """
        Determine the number of vehicles with drivers having valid licenses involved in hit-and-run incidents.
        """
        result = (self.unit_df
                  .join(self.primary_person_df, ["CRASH_ID"])
                  .filter((col("VEH_HNR_FL") == "Y") & (col("DRVR_LIC_TYPE_ID").isin(
                        ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
                    )))
                  .select("CRASH_ID")
                  .distinct()).count()
        
        df = self.spark.createDataFrame([{"NO_OF_VEHICLES":result}])

        logger.info(f"Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? \nAnswer - {result}")
        write_df_as_csv(df, self.output_path+'/4')
        
        return result


    def find_state_with_accidents_without_females_involved(self):
        """
        Find the state with the highest number of accidents where females are not involved.
        """
        # Filter out all crashes where there were no females involved 
        accidents_without_females = self.primary_person_df.groupBy('CRASH_ID').\
            pivot("PRSN_GNDR_ID",["MALE","FEMALE"]).count().fillna(0).filter(col("FEMALE") == 0)
        
        
        df = (self.primary_person_df
                  .join(accidents_without_females, on="CRASH_ID", how="inner")
                  .groupBy("DRVR_LIC_STATE_ID")
                  .agg(count("CRASH_ID").alias("accident_count"))
                  .orderBy(desc("accident_count"))
                  .limit(1))
        
        result = df.collect()[0][0]
        logger.info(f"Analysis 5: Which state has highest number of accidents in which females are not involved?\nAnswer - {result}")
        write_df_as_csv(df, self.output_path+'/5')
        
        return result


    def find_3to5_vehicle_with_most_injuries_or_death(self):
        """
        Find the 3rd to 5th vehicle makes contributing to the largest number of injuries including death.
        """
        window = Window.orderBy(desc("TOTAL"))

        df = (
            self.unit_df.filter(self.unit_df.VEH_MAKE_ID != "NA")
            .withColumn("TOTAL", col("TOT_INJRY_CNT") + col("DEATH_CNT"))
            .groupby("VEH_MAKE_ID")
            .agg(sum("TOTAL").cast('int').alias("TOTAL"))
            .withColumn("rank", row_number().over(window))
                  .filter((col("rank") >= 3) & (col("rank") <= 5))).drop("rank")

        result = [row[0] for row in df.collect()]
        logger.info(f"Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death?\nAnswer - {result}")
        write_df_as_csv(df, self.output_path+'/6')
        
        return result


    def find_top_ethnic_ug_crash_for_each_body_style(self):
        """
        For each body style involved in crashes, find the top ethnic user group.
        """
        join_condition = self.unit_df.CRASH_ID == self.primary_person_df.CRASH_ID

        joined_df = self.unit_df.join(self.primary_person_df, join_condition, "inner") \
            .select(self.unit_df.CRASH_ID,
                    self.unit_df.VEH_BODY_STYL_ID,
                    self.primary_person_df.PRSN_ETHNICITY_ID)

        # Total Crashes for a body style
        top_body_styles = joined_df.\
            groupBy(self.unit_df.VEH_BODY_STYL_ID, self.primary_person_df.PRSN_ETHNICITY_ID)\
            .count() \
            .orderBy(col("count").desc())

        window = Window.\
            partitionBy("VEH_BODY_STYL_ID")\
            .orderBy(col("count").desc())

        # Top Ethnicites for each body style of the vehicle involved in the crash
        df = top_body_styles.\
            withColumn("rank", dense_rank().over(window)) \
            .filter(col("rank") == 1)\
            .drop("rank", "count")

        result = {row[0]:row[1] for row in df.collect()}
        logger.info(f"Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style.\nAnswer - {result}")
        write_df_as_csv(df, self.output_path+'/7')  
        
        return result

    

    def find_top5_zipcodes_with_highest_car_crashes_involving_alcohol(self):
        """
        Find the top 5 ZIP codes with the highest number of car crashes where alcohol was a contributing factor.
        """

        cars = ['PASSENGER CAR, 4-DOOR', 'SPORT UTILITY VEHICLE', 'PASSENGER CAR, 2-DOOR', 'POLICE CAR']
        filter_crashes_with_cars_only = self.unit_df.filter(col("VEH_BODY_STYL_ID").isin(cars)).select("CRASH_ID")

        df = (filter_crashes_with_cars_only
                  .join(self.primary_person_df, ["CRASH_ID"])
                  .filter((col("PRSN_ALC_RSLT_ID") == "Positive") & (col("DRVR_ZIP").isNotNull()))
                  .groupBy("DRVR_ZIP")
                  .agg(count("CRASH_ID").alias("CRASH_COUNT"))
                  .orderBy(desc("CRASH_COUNT"))
                  .limit(5))
        
        result = {row[0]:row[1] for row in df.collect()}
        logger.info(f"Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)\nAnswer - {result}")
        write_df_as_csv(df, self.output_path+'/8')
        
        return result


    def count_crash_ids_without_any_damage(self):
        """
        Count distinct crash IDs where no damaged property was observed, damage level is above 4, and the car avails insurance.
        """
       
        df = (
            self.damages_df.join(self.unit_df, on=["CRASH_ID"], how="inner")
            .filter(
                (
                    ((regexp_extract("VEH_DMAG_SCL_1_ID", '\d+', 0) > 4))
                    & (
                        ~self.unit_df.VEH_DMAG_SCL_1_ID.isin(
                            ["NA", "NO DAMAGE", "INVALID VALUE"]
                        )
                    )
                )
                | (
                    ((regexp_extract("VEH_DMAG_SCL_2_ID", '\d+', 0) > 4))
                    & (
                        ~self.unit_df.VEH_DMAG_SCL_2_ID.isin(
                            ["NA", "NO DAMAGE", "INVALID VALUE"]
                        )
                    )
                )
            )
            .filter(self.damages_df.DAMAGED_PROPERTY == "NONE")
            .filter(self.unit_df.FIN_RESP_TYPE_ID.like("%INSURANCE%"))
        ).select("CRASH_ID").distinct()
        
        result = df.count()
        logger.info(f"Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance.\nAnswer - {result}")
        write_df_as_csv(df, self.output_path+'/9')
        
        return result

    
    def find_top5_vehicle_brands_with_speeding_offense(self):
        """
        Find the top 5 vehicle makes where drivers are charged with speeding-related offenses, have valid licenses, use the top 10 vehicle colors, and are licensed in the top 25 states.
        """
    

        # Drivers with license charged with speed related offense
        join_condition = self.primary_person_df.CRASH_ID == self.charges_df.CRASH_ID
        speeding_with_licences = self.charges_df.join(self.primary_person_df, join_condition)\
            .filter((col("CHARGE").like("%SPEED%")) &
                   (col("DRVR_LIC_TYPE_ID") == "DRIVER LICENSE")).select(self.charges_df.CRASH_ID,
                    self.primary_person_df.DRVR_LIC_TYPE_ID,
                    self.primary_person_df.DRVR_LIC_STATE_ID
                    )
        
        # Top 25 states with highest number of offences
        top_states_offences = self.primary_person_df\
            .groupBy("DRVR_LIC_STATE_ID")\
            .count()\
            .orderBy(col("count").desc()).take(25)

        # Top 10 used vehicles colours with licenced
        top_licensed_vehicle_colors = self.unit_df.\
            join(self.primary_person_df, self.unit_df.CRASH_ID == self.primary_person_df.CRASH_ID) \
            .filter((col("DRVR_LIC_TYPE_ID") == "DRIVER LICENSE")) \
            .groupBy("VEH_COLOR_ID")\
            .count()\
            .orderBy(col("count").desc()).take(10)

        top_colors = [row["VEH_COLOR_ID"] for row in top_licensed_vehicle_colors]
        top_states = [row['DRVR_LIC_STATE_ID'] for row in top_states_offences]

        top_vehicles_made = speeding_with_licences\
            .join(self.unit_df, speeding_with_licences.CRASH_ID == self.unit_df.CRASH_ID, "inner") \
            .filter((col("VEH_COLOR_ID").isin(top_colors))
                   & (col("DRVR_LIC_STATE_ID").isin(top_states))) \
            .select("VEH_MAKE_ID").groupBy("VEH_MAKE_ID").count().orderBy(col("count").desc())\
            .limit(5)
        
        result = [row[0] for row in top_vehicles_made.collect()]
        logger.info(f"Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours andhas car licensed with the Top 25 states with highest number of offences (to be deduced from the data).\nAnswer - {result}")
        write_df_as_csv(top_vehicles_made, self.output_path+'/10')
        top_vehicles_made.show()
        return result
        
