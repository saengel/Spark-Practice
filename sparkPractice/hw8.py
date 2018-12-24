import pyspark

# Setting up spark context
                                                           
sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")

# Question:
# The list of unique donors (name/city) who gave an individual
# donation of exactly $25000. The names must be written to a file.

textData = sc.textFile("NYfec.gz")

# Step 1: Map to unique name and donation
# Mapper to get unique name and how much donated           
# param x = each line of the file                          
def getNameDonation(x):

    fields = x.split("|")
    name = fields[7]   

    city = fields[8]
    uniqueName = name + city

    donation = fields[14]

    return (uniqueName, donation)

a = textData.map(getNameDonation)

# Filter by donations == "25000"
b = a.filter(lambda x: x[1] == "25000")

# Map only to the names, and distinct-ify
c = b.map(lambda x: x[0]).distinct()

# Save to a file
# Giving it a folder to store it in the pwd
# part-0000 will be the name of the file
c.saveAsTextFile("answer8")
