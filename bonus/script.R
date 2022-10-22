options(stringsAsFactors = FALSE)
#loading data
Flights_2005 <- read.csv("2005.csv.bz2")
Flights_2006 <- read.csv("2006.csv.bz2")
Flights_2007 <- read.csv("2007.csv.bz2")
Flights_2008 <- read.csv("2008.csv.bz2")
Flights <- rbind(Flights_2005[,c("Year", "DayOfWeek", "DepTime", "ArrTime", "UniqueCarrier", "TailNum", "ArrDelay", "DepDelay", "Origin", "CancellationCode")],
                 Flights_2006[,c("Year", "DayOfWeek", "DepTime", "ArrTime", "UniqueCarrier", "TailNum", "ArrDelay", "DepDelay", "Origin", "CancellationCode")],
                 Flights_2007[,c("Year", "DayOfWeek", "DepTime", "ArrTime", "UniqueCarrier", "TailNum", "ArrDelay", "DepDelay", "Origin", "CancellationCode")],
                 Flights_2008[,c("Year", "DayOfWeek", "DepTime", "ArrTime", "UniqueCarrier", "TailNum", "ArrDelay", "DepDelay", "Origin", "CancellationCode")])
remove(Flights_2005)
remove(Flights_2006)
remove(Flights_2007)
remove(Flights_2008)
#write.csv(Flights, file=bzfile("flights.csv.bz2"))   # saving only necessary data
#Flights <- read.csv("flights.csv.bz2")



library('sqldf')





##### --- 1) #####
#Depending on a day (Monday,...,Sunday), what is
#the probability that a plane will be delayed?
#Take into account 'ArrDelay' as it is more important
#for passengers to be on time where they want,
#then what time they departed.



query1 <- sqldf("
SELECT
DayOfWeek,
(SUM(CASE WHEN ArrDelay > 0 THEN 1 ELSE 0 END)) as NoOfArrDelayed,
COUNT(*) as TotalFlights,
(CAST(SUM(CASE WHEN ArrDelay > 0 THEN 1 ELSE 0 END)*100 AS float)/CAST(COUNT(*) AS float)) as PercentageOfArrDelayed
FROM Flights
GROUP BY DayOfWeek
")



##### --- 2) #####
#From which airport there is the biggest average
#arrival delay in the destination airport?




query2 <- sqldf("
SELECT
Origin,
AVG(ArrDelay) AS AverageArrDelay
FROM Flights
WHERE ArrDelay>0
GROUP BY Origin
ORDER BY AverageArrDelay DESC
")



#From which airport there is the biggest average
#early arrival in the destination airport?



query2_2 <- sqldf("
SELECT
Origin,
AVG(ArrDelay) AS AverageArrEarly
FROM Flights
WHERE ArrDelay<0
GROUP BY Origin
ORDER BY AverageArrEarly ASC
")



##### --- 3) #####
#Based on the scheduled departure time (timeslots of 1h for example)
#what is the probability that a flight will be late at
#destination airport atleast 30minutes?



query3 <- sqldf("SELECT
Cast(Hour AS INT) AS Hour,
CAST((CAST(DelayedOver30*100 AS float)/CAST(TotalFlights AS float)) AS float) AS Percent
FROM
(
SELECT
CASE
WHEN LENGTH(DepTime)=4 THEN SUBSTRING(DepTime, 1, 2)
ELSE SUBSTRING(DepTime, 1, 1)
END AS Hour,
Count(*) as TotalFlights,
SUM(CASE WHEN ArrDelay > 29 THEN 1 ELSE 0 END) as DelayedOver30
FROM Flights
WHERE Hour!='<NA>'
GROUP BY Hour
)
ORDER BY Hour ASC
")





##### --- 4) #####
#If the 'ArrDelay' occurrs, present an average
#delay for every day of the week
query4 <- sqldf("
SELECT
DayOfWeek,
AVG(ArrDelay) AS AverageArrDelay,
AVG(DepDelay) AS AverageDepDelay
FROM Flights
WHERE ArrDelay>0
GROUP BY DayOfWeek
ORDER BY DayOfWeek ASC
")



##### --- 5) #####
#Show which plane flew the most flights
#in each year



query5 <- sqldf("SELECT
Year,
TailNum,
MAX(Liczba) as MostFlights
FROM
(
SELECT
Year,
TailNum,
Count(*) as Liczba
from Flights
WHERE TailNum!='0' and TailNum!='000000' and TailNum!=''
GROUP BY Year,TailNum
)
GROUP BY Year
")




##### --- 6) #####
# What were top carrier companies in number of cases
# that their flights got cancelled because of
# security reasons compared to amount of flights in total for this carrier?



query6 <- sqldf("SELECT
UniqueCarrier,
NumberOfFlights,
NumberOfSecurityIncidents,
CAST((CAST(NumberOfSecurityIncidents AS float)/CAST(NumberOfFlights AS float)) AS float) AS Percent
FROM
(
SELECT
UniqueCarrier,
Count(*) as NumberOfFlights,
(SUM(CASE WHEN CancellationCode='D' THEN 1 ELSE 0 END)) as NumberOfSecurityIncidents
from Flights
GROUP BY UniqueCarrier
)
ORDER BY Percent DESC
")