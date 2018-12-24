import pyspark

# Setting up spark context                                             
sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")

# QUESTION:
# How many ZIP codes were the source of donations that were no higher
# than $100

# PSEUDO:
# Make a set of all ZIP codes that had any donations
# - map, distinct (transform to just one of each)
# Make a set of all ZIPs that had donations > 100
# - map, filter, distinct
# Subtract the second set from the first set
# Count the resulting set

# Ingest the data
dataFile = sc.textFile("NYfec.gz")

# MAPPER
# Split the record, extract the ZIP field, and take
# the first five digits.
# And only want a distinct set, so apply distinct()
set1 = dataFile.map(lambda x: x.split("|")[10][:5]).distinct()

# function for set2
# x is a line of text
def getZipDollars(x):
    fields = x.split("|")
    zipcode = fields[10][:5]
    money = fields[14]
    try: dollars = int(money)
    except: dollars = 0
    return (zipcode, dollars)

# Filter only those donations                         
# greater than 100
# Then extract only the zip codes
# and distinct()
set2 = dataFile.map(getZipDollars)\
               .filter(lambda x: x[1] > 100)\
               .map(lambda x: x[0]).distinct()

# Subtract set2 from set1 to find the zip codes
# that had all donations <=100
# And count all the remaining ones
d = set1.subtract(set2).count()

print("There were", d, "zip codes that had all donations <= 100")
