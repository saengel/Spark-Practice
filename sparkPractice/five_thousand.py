# Set up the spark context                                                        
import pyspark

sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")

# Bring in the data into the program                                              
textData = sc.textFile("NYfec.gz")

# QUESTION:
# Number of donations exactly 5000
# Plan
# - map emit just donations field (using a lambda)
# - filter out only those == 5000
# - count()

a = textData.map(lambda x: x.split("|")[14])\
            .filter(lambda x: x == "5000")\
            .count()

print("Total number of $5,000 donations = ", a)
