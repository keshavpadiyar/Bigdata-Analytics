#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark import SparkContext
import pyspark.sql.functions as F


# In[ ]:


spark = SparkContext("local", "Lab2")


# In[ ]:


# Assignment 1: What are the lowest and highest temperatures measured each year for the period 1950-2014.
# Using Dataframes
df_tempReadings = spark.read.csv("file:///home/x_kesma/Lab1/input_data/temperature-readings.csv", header = False, sep = ';' )
df_tempReadings = df_tempReadings.withColumnRenamed("_c0", "stationNumber")                                 .withColumnRenamed("_c1", "date")                                 .withColumnRenamed("_c2", "time")                                 .withColumnRenamed("_c3", "airTemperature")                                 .withColumnRenamed("_c4", "quality")

df_filtered = df_tempReadings.filter((F.year(F.col('date'))>=1950) & (F.year(F.col('date'))<=2014))

df_filtered.groupBy(F.year('date'))            .agg(F.min('airTemperature').alias('MinTemp'),F.max('airTemperature').alias('MaxTemp'))            .orderBy(F.year('date'))            .show(100)

                                


# In[ ]:


#using RDD
rdd_tempReadings = sc.textFile("file:///home/x_kesma/Lab1/input_data/temperature-readings-small.csv")                             .map(lambda line: line.split(";"))

rdd_filtered_1 = rdd_tempReadings.filter(lambda line: (int(line[1][0:4]))>=1950 and int(line[1][0:4])<=2014)                             .map(lambda line: (line[1][0:4],(line[0],float(line[3]))))

print(rdd_filtered_1.reduceByKey(max)            .sortBy(keyfunc=lambda k: k[0],ascending = False).collect())

print(rdd_filtered_1.reduceByKey(min)            .sortBy(keyfunc=lambda k: k[0],ascending = False).collect())


# [(u'2014', (u'99450', 26.0)), (u'2013', (u'99450', 21.6)), (u'2012', (u'99450', 21.3)), (u'2011', (u'99450', 21.8)), (u'2010', (u'99450', 25.2)), (u'2009', (u'99450', 22.4)), (u'2008', (u'99450', 24.7)), (u'2007', (u'99450', 21.5)), (u'2006', (u'99450', 24.5)), (u'2005', (u'99450', 25.7)), (u'2004', (u'99450', 25.1)), (u'2003', (u'99450', 25.5)), (u'2002', (u'99450', 24.7)), (u'2001', (u'99450', 24.7)), (u'2000', (u'99450', 18.8)), (u'1999', (u'99450', 24.1)), (u'1998', (u'99450', 19.6)), (u'1997', (u'99450', 25.5)), (u'1996', (u'99450', 23.7)), (u'1995', (u'99450', 24.1)), (u'1994', (u'99450', 25.7)), (u'1993', (u'99450', 19.1)), (u'1992', (u'99450', 22.6)), (u'1991', (u'99450', 25.1)), (u'1990', (u'99450', 22.3)), (u'1989', (u'99450', 24.2)), (u'1988', (u'99450', 22.3)), (u'1987', (u'99450', 22.6)), (u'1986', (u'99450', 22.1)), (u'1985', (u'99450', 19.7)), (u'1984', (u'99450', 21.7)), (u'1983', (u'99450', 25.8)), (u'1982', (u'99450', 23.7)), (u'1981', (u'99450', 21.1)), (u'1980', (u'99450', 24.2)), (u'1979', (u'99450', 19.2)), (u'1978', (u'99450', 22.7)), (u'1977', (u'99450', 18.8)), (u'1976', (u'99450', 21.6)), (u'1975', (u'99450', 25.6)), (u'1974', (u'99450', 19.8)), (u'1973', (u'99450', 23.8)), (u'1972', (u'99450', 24.0)), (u'1971', (u'99450', 21.0)), (u'1970', (u'99450', 21.0)), (u'1969', (u'99450', 26.0)), (u'1968', (u'99450', 22.8)), (u'1967', (u'99450', 21.6)), (u'1966', (u'99450', 24.0)), (u'1965', (u'99450', 19.0)), (u'1964', (u'99450', 22.0)), (u'1963', (u'99450', 21.0)), (u'1962', (u'99450', 18.2)), (u'1961', (u'99450', 20.3)), (u'1960', (u'99450', 19.8)), (u'1959', (u'99450', 23.5)), (u'1958', (u'99450', 19.4)), (u'1957', (u'99450', 21.0)), (u'1956', (u'99450', 19.1)), (u'1955', (u'99450', 24.6)), (u'1954', (u'99450', 19.6)), (u'1953', (u'99450', 23.0)), (u'1952', (u'99450', 20.4)), (u'1951', (u'99450', 20.8)), (u'1950', (u'98610', 26.2))]
# 

# In[ ]:


# 2_1 Count the number of readings for each month in the period of 1950-2014 which are higher than 10 degrees 

rdd_filtered_2_1 = rdd_tempReadings.filter(lambda line: ((int(line[1][0:4]))>=1950                                                       and int(line[1][0:4])<=2014)                                                       and float(line[3]) >10 )                                .map(lambda line: ((line[1][0:4], line[1][5:7]),(line[0],float(line[3]))))                                .countByKey()
print(sorted(rdd_filtered_2_1.items(), key = lambda v:v[1], reverse = True))


# In[ ]:


# 2_2 Repeat the exercise,this time taking only distinct readings from each station.
# That is, if a station reported a reading above 10 degrees in some month, then itappears only
# once in the count for that month

rdd_filtered_2_2 = rdd_tempReadings.filter(lambda line: ((int(line[1][0:4]))>=1950                                                       and int(line[1][0:4])<=2014)                                                       and float(line[3]) >10 )                                .map(lambda line: (line[1][0:4], line[1][5:7],line[0]))                                .distinct()                                .map(lambda line: ((line[0],line[1]),(line[2])))                                .countByKey()
print(sorted(rdd_filtered_2_2.items(), key = lambda v:v[1], reverse = True))


# In[ ]:


# 3 Find the average monthly temperature for each available station in Sweden. Your result
#should include average temperature for each station for each month in the period of 1960-
#2014. Bear in mind that not every station has the readings for each month in this timeframe.

rdd_filtered_3 = rdd_tempReadings.filter(lambda line: (int(line[1][0:4]))>=1950 and int(line[1][0:4])<=2014)                             .map(lambda line: ((line[1][0:4], line[1][5:7], line[0]),(float(line[3]))))                            .groupByKey()                            .mapValues(lambda val: sum(val)/len(val))
print(rdd_filtered_3            .sortBy(keyfunc=lambda k: (k[0][2],k[0][0],k[0][1]),ascending = False).collect())


# In[1]:


# 4 Provide a list of stations with their associated maximum measured temperatures and
# maximum measured daily precipitation. Show only those stations where the maximum
# temperature is between 25 and 30 degrees and maximum daily precipitation is between 100mm and 200mm.
rdd_precReadings = sc.textFile("file:///home/x_kesma/Lab1/input_data/precipitation-readings.csv")                             .map(lambda line: line.split(";"))                            #.filter(lambda line: int(line[3]>=100 and float(line[3])<=200))

rdd_precReadings.collect()

