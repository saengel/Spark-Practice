# Set up the spark context
import pyspark

sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")

# Bring in the data into the program
textData = sc.textFile("NYfec.gz")

# QUESTION
# - number of unique ZIP codes
# Map out the zip codes
# Distinct them
# Count them

# Defining a function to pass into map
# to extract just the zip codes
def getZip(x):
    fields = x.split("|")
    zipcode = fields[10]
    return zipcode

# Map to zip and distinctify
a = textData.map(getZip).distinct()

# test = a.take(10)
# print("TEST")
# print(test)

# Now count them
c = a.count()

print("Total number of zip codes is = " + str(c))
