# Set up the spark context

# FIELDS:                                                           # Name = F7                                                         # City = F8
# ZIP = F10                                                         # Date = F13                                                        # $ = F14                                                                
import pyspark

sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")

# Bring in the data into the program                                     
textData = sc.textFile("NYfec.gz")                                       
# QUESTION:
# the number of unique donors (name/city) who gave at least one
# $5000 donation and one $10000 donation

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

# Now, how to I make sure they gave both?
# Maybe make a set of 5000 with unique names
# and 10000 of unique names and take their intersection...

def fiveThous(x):
    donation = x[1]
    if donation == 5000:
        return True
    else:
        return False
    
set_fiveThous = a.filter(fiveThous)

def tenThous(x):
    donation = x[1]
    if donation == 10000:
        return True
    else:
        return False

set_tenThous = a.filter(tenThous)

# Map both sets just to their names and then distinct

c = set_tenThous.map(lambda x: x[0]).distinct()
d = set_fiveThous.map(lambda x: x[0]).distinct()

# Taking the intersection of the two distinct sets of names
intersection = c.intersection(d)

# Counting the intersection
ans = intersection.count()

print("Distinct number of unique names with 5000 and 10000 donations =", ans)
