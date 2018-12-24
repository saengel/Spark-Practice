import pyspark

# Setting up spark context
sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")

# Get text into the system
textData = sc.textFile("NYfec.gz")

# QUESTION:
# The sum of all dollars

# MAPPER PHASE
# Map to just the dollars

# Define function to extract
# just the dollar field for each line
def getDollars(x):
    fields = x.split("|")
    money = fields[14]

    try: dollars = int(money)
    except: dollars = 0

    return dollars

a = textData.map(getDollars)

# Sum all the dollars

result = a.sum()

print("The sum of all dollars = " + str(result))
