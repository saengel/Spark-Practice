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
# the number of unique donors (name/city) who gave at least one
# donation of $5000 or more

# Mapper to get unique name and how much donated
# param x = each line of the file
def getNameDonation(x):
    fields = x.split("|")
    name = fields[7]
    city = fields[8]
    uniqueName = name + city
    try: donation = int(fields[14])
    except: donation = 0
    return (uniqueName, donation)

a = textData.map(getNameDonation)

# Transform function - where x is a tuple
def get5000(x):
    donation = x[1]
    if donation >= 5000:
        return True
    else:
        return False
    
b = a.filter(get5000)

# Map by name
c = b.map(lambda x: x[0])

# Distinctify the answer, and then count

ans = c.distinct().count()

print("Total number of unique donors who donated exactly 5000 or more is", ans)
