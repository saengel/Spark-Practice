import pyspark

# Setting up spark context                                             
sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")

# Question: Largest refund dollar amount for a single contribution
# in 2016.
# - will need to map both the refund and the year
# - mapper will need to create a tuple
# then will transform to just 2016
# then mapper to just extract dollar

dataFile = sc.textFile("NYfec.gz")

# Need to extract year and dollar field in mapper
# so make a function to pass to map()

# Param x = line of text
def getYearDollar(x):
    fields = x.split("|")
    year = fields[13][-4:]
    money = fields[14]
    
    # Have to make sure money is real
    try: dollars = int(money)
    except: dollars = 0

    # Returning tuple of year and integer-ized
    # money
    return (year, dollars)

b = dataFile.map(getYearDollar)

# Now, only want tuples that contain the year 2016
# lambda = returning x if the first field
# of the generated tuples is equal to 2016
# note string...
# and ensure we're only getting negative
# refund values
c = b.filter(lambda x: x[0] == "2016" and x[1] < 0)

# Now write a mapper to just get dollar values
# from each tuple
# Now have an RDD with just refund dollars from 2016
d = c.map(lambda x: x[1])

# Now reducing the entire RDD
# Taking the min of every two values
# globally reducing behind the scenes
# to find the most negative (aka highest
# refund value) of the file
# implied - since it's a global operation
# a sort and shuffle had to have happened
# behind the scenes.

# Note: reduce() is a data retrieval
# puts everything together, e is our
# answer
e = d.reduce(lambda x,y: min(x,y))

print("The largest refund in 2016 was", e)

# WRITING THE SAME THING IN "ZEN"
# a = sc.textFile("NYfec.gz")\
#    .map(getYearDollar)
#    .filter(lambda x: x[0] == "2016" and x[1] <0)
#    .map(lambda x: x[1])
#    .reduce(lambda x,y: min(x,y))
