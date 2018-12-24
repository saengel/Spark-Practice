import pyspark

# Setting up spark context
sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")

# SPARK CONCEPTUAL REVIEW
# Take text lines and pass them through the mapper
# splitting on "|"
# Transformation - take just the field we want
# Action - pulls it back to the cluster

# EXAMPLE PROBLEM ONE

# First step ALWAYS: Use textFile() to get the data into
# our program. (For iterables, generate numbers and
# parallelize() them)
textData = sc.textFile('NYfec.gz')

# MAPPER PHASE

# function to pass through mapper (can use a lambda)
def getYear(x):
    # x = line from the input file
    fields = x.split("|")
    date = fields[13]
    year = date[-4:] # Extract the last four characters from the date, just the year
    return year
    # In a lambda, would have written as
    # (lambda x: x.split("|")[13][-4:])
    # aka return x for x split on pipes, extracting the
    # 13th field, and then the last four characters
    # of that 13th field. 

# Mapper - invoke map on data
# Just passing the function in without param
# because the map function applies the function inside
# automatically to each line of the text
b = textData.map(getYear)

# Print statement to check
# Just inhaling the first ten
print("\nTEST MAPPER:")
print(b.take(10))

# TRANSFORMATION PHASE
# Need to filter out only the years
# that are 2003 from our mapped "b"
# Have to pass either a lambda or
# function that returns either T or F
# for keeping it in the RDD

# Return x if x == "2003" (note:
# using a string because it's
# a text file)
c = b.filter(lambda x: x == "2003")


# Data Retrieval ACTION phase
d = c.count()
print("\nThere are", d, "records with year = 2003 in the file")

# Run the file in CLI
# spark-submit nameOfFile.py

