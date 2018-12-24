# GETTING AN EXTRA NONE -REALLY WEIRD... 

# Set up the spark context
# FIELDS:
# Name = F7
# City = F8
# ZIP = F10
# Date = F13
# $ = F14

import pyspark

sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")

# Bring in the data into the program                                              
textData = sc.textFile("NYfec.gz")

# QUESTION:
# The number of unique donors from Jericho
# who gave at least one donation of exactly $100

# PLAN:
# Map tuple of name, city and data
# filter on city == Jericho
# Map by donations of exactly 100 emit the name
# distinct the names

# MAPPER
def getNameCityDollar(x):
    fields = x.split("|")
    name = fields[7]
    city = fields[8]
    money = fields[14]

    return(name, city, money)

a = textData.map(getNameCityDollar)

# Filtering on city
b = a.filter(lambda x: x[1] == "JERICHO")

# TEST CODE FOR FILTER
# z = b.take(10)
# print("Testing if filter on city works")
# print(z)

# Mapping on $100
def is100(x):
    dollars = x[2]
    if dollars == "100":
        if x[0] != None or x[0] != "":
            return x[0] # returning the name

c = b.map(is100)

# Distinctify the names and count them
d = c.distinct().count()

# TEST CODE
# e = d.take(32)
# print(e)

print("The total number of people from Jericho with an 100 donation = " + str(d))
