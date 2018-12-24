# FIX - result should be: 119644


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

# Question
# the number of unique donors (name/city)
# who gave at least one single donation greater
# than 150% of the overall average donation.


# MAPPER - map to unique name and donation
def getNameDonation(x):                                   

    fields = x.split("|")
    name = fields[7]                                      

    city = fields[8]
    uniqueName = name + city                              

    try: donation = int(fields[14])
    except: donation = 0
    return (uniqueName, donation)

a = textData.map(getNameDonation)

# Action to find the average donation
# first map to donations
def getDonation(x):    
    fields = x.split("|")
    try: donation = int(fields[14])
    except: donation = 0
    return donation

donations = textData.map(getDonation)
avg = donations.mean()
threshold = avg * 1.5

print("Avg =", avg)

# Filter the results of mapper based on percent
# of average - x is a tuple of name, donation
def getPercent(x):
    # print("Percent = ", str(percent))
    if x[1] > threshold:
        print("Donation of " + str(x[1]) + " exceeds threshold of " + str(threshold))
        return True
    else:
        return False

b = a.filter(getPercent)
d = b.take(10)
print("Testing:", d)

# Map all filtered by donations to name
# and then distinctify
c = b.map(lambda x: x[0]).distinct()

# Action to count those from the filter
result = c.count()

print("Total = ", result)
