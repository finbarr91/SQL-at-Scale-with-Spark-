import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
spark = SparkSession.builder.appName("Sparkminiproject").getOrCreate()

# File location and type
file_location_bookings = r"C:\Users\chukw\PycharmProjects\SQL-at-Scale-with-Spark-\Bookings.csv"
file_location_facilities = r"C:\Users\chukw\PycharmProjects\SQL-at-Scale-with-Spark-\Facilities.csv"
file_location_members = r"C:\Users\chukw\PycharmProjects\SQL-at-Scale-with-Spark-\Members.csv"

file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
bookings_df = (spark.read.format(file_type)
                    .option("inferSchema", infer_schema)
                    .option("header", first_row_is_header)
                    .option("sep", delimiter)
                    .load(file_location_bookings))

facilities_df = (spark.read.format(file_type)
                      .option("inferSchema", infer_schema)
                      .option("header", first_row_is_header)
                      .option("sep", delimiter)
                      .load(file_location_facilities))

members_df = (spark.read.format(file_type)
                      .option("inferSchema", infer_schema)
                      .option("header", first_row_is_header)
                      .option("sep", delimiter)
                      .load(file_location_members))

# Infer the schema, and register the DataFrame as a table.

bookings_df.createOrReplaceTempView("bookings_df")
facilities_df.createOrReplaceTempView('facilities_df')
members_df.createOrReplaceTempView("members_df")

# SQL can be run over DataFrames that have been registered as a table.
book = spark.sql("SELECT * FROM bookings_df LIMIT 3")

book.show()


#Q1. Some of the facilities charge a fee to members, but some do not.
# Please list the names of the facilities that do.
print("LIst of names of facilities that charge a fee to members")
facilities_that_charge_a_fee = spark.sql("SELECT name FROM facilities_df\
                                        WHERE membercost != 0.0")
facilities_that_charge_a_fee.show()


# Q2: How many facilities do not charge a fee to members?
print("Count of facilities do not charge a fee to members")
count_of_facilities_that_do_not_charge_a_fee = spark.sql("SELECT COUNT(DISTINCT(name)) FROM facilities_df\
                                                         WHERE membercost =0.0")
count_of_facilities_that_do_not_charge_a_fee.show()

# Q3: How can you produce a list of facilities that charge a fee to members,
# where the fee is less than 20% of the facility's monthly maintenance cost?
print("list of facilities that charge a fee to members, where the fee is less than 20% of the facility's monthly maintenance cost")
facilities_that_charge_a__fee_where_the_fee_is_less_than_20percent_of_their_monthly_maintenance = spark.sql("SELECT name FROM facilities_df\
                                                                                                            WHERE membercost != 0.0 AND membercost<0.2*monthlymaintenance")
facilities_that_charge_a__fee_where_the_fee_is_less_than_20percent_of_their_monthly_maintenance.show()


# Q4: How can you retrieve the details of facilities with ID 1 and 5?
# Write the query without using the OR operator.
SELECT * FROM facilities
WHERE facid IN (1, 5)


# Q5: How can you produce a list of facilities, with each labelled as 'cheap' or 'expensive',
# depending on if their monthly maintenance cost is more than $100?
# Return the name and monthly maintenance of the facilities in question.
print("list of facilities, with each labelled as 'cheap' or 'expensive',depending on if their monthly maintenance cost is more than $100? "
  "Return the name and monthly maintenance of the facilities in question")
SELECT name, monthlymaintenance,
CASE WHEN monthlymaintenance > 100 THEN 'expensive'
     ELSE 'cheap' END AS label
FROM Facilities

# Q6: You'd like to get the first and last name of the last member(s) who signed up.
# Do not use the LIMIT clause for your solution.
SELECT firstname, surname
FROM Members
WHERE joindate = (
SELECT MAX(joindate)
FROM Members)


# Q7: How can you produce a list of all members who have used a tennis court?
# Include in your output the name of the court, and the name of the member formatted as a single column.
# Ensure no duplicate data
# Also order by the member name.
SELECT sub.court, CONCAT( sub.firstname,  ' ', sub.surname ) AS name
FROM (
SELECT Facilities.name AS court, Members.firstname AS firstname, Members.surname AS surname
FROM Bookings
INNER JOIN Facilities ON Bookings.facid = Facilities.facid
AND Facilities.name LIKE  'Tennis Court%'
INNER JOIN Members ON Bookings.memid = Members.memid
) sub
GROUP BY sub.court, sub.firstname, sub.surname
ORDER BY name


# Q8: How can you produce a list of bookings on the day of 2012-09-14 which will cost the member (or guest) more than $30?
# Remember that guests have different costs to members (the listed costs are per half-hour 'slot')
# The guest user's ID is always 0.
# Include in your output the name of the facility, the name of the member formatted as a single column, and the cost.
# Order by descending cost, and do not use any subqueries.

SELECT Facilities.name AS facility, CONCAT( Members.firstname,  ' ', Members.surname ) AS name,
CASE WHEN Bookings.memid =0
THEN Facilities.guestcost * Bookings.slots
ELSE Facilities.membercost * Bookings.slots
END AS cost
FROM Bookings
INNER JOIN Facilities ON Bookings.facid = Facilities.facid
AND Bookings.starttime LIKE  '2012-09-14%'
AND (((Bookings.memid =0) AND (Facilities.guestcost * Bookings.slots >30))
OR ((Bookings.memid !=0) AND (Facilities.membercost * Bookings.slots >30)))
INNER JOIN Members ON Bookings.memid = Members.memid
ORDER BY cost DESC

# Q9: This time, produce the same result as in Q8, but using a subquery.
SELECT *
FROM (
SELECT Facilities.name AS facility, CONCAT( Members.firstname,  ' ', Members.surname ) AS name,
CASE WHEN Bookings.memid =0
THEN Facilities.guestcost * Bookings.slots
ELSE Facilities.membercost * Bookings.slots
END AS cost
FROM Bookings
INNER JOIN Facilities ON Bookings.facid = Facilities.facid
AND Bookings.starttime LIKE  '2012-09-14%'
INNER JOIN Members ON Bookings.memid = Members.memid
)sub
WHERE sub.cost >30
ORDER BY sub.cost DESC


# Q10: Produce a list of facilities with a total revenue less than 1000.
# The output should have facility name and total revenue, sorted by revenue.
# Remember that there's a different cost for guests and members!
SELECT *
FROM (
SELECT sub.facility, SUM( sub.cost ) AS total_revenue
FROM (
SELECT Facilities.name AS facility,
CASE WHEN Bookings.memid =0
THEN Facilities.guestcost * Bookings.slots
ELSE Facilities.membercost * Bookings.slots
END AS cost
FROM Bookings
INNER JOIN Facilities ON Bookings.facid = Facilities.facid
INNER JOIN Members ON Bookings.memid = Members.memid
)sub
GROUP BY sub.facility
)sub2
WHERE sub2.total_revenue <1000


spark.stop()