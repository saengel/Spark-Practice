import pyspark                                             
                                                           
sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")                                    
 
# Bring in the data into the program                                      
textData = sc.textFile("NYfec.gz")

# QUESTION:
# The number of unique donors who gave individual donations of $10000 or more
# but didn’t give any donation of $1000 or less.

# STRATEGY
# Map to unique name and donation
#
# SET ONE:
# Make one filtered set of those who gave 10000 or more
# Then map again just to name
# and distinctify = set of unique 10000 or more donors
#
# SET TW0:
# Make one filtered set of those who gave 1000 or less 
# Then map again just to name                        
# and distinctify = set of unique less than 1000 donors
#
# Make an intersection of the two sets
# And count - those are the people who we're disqualifying
# from our first set
# Count SET ONE and subtract our intersection = TOTAL
# (People who gave over 10000) - (People who also gave less than 1000) =
# Total: People who only gave over 10000

# Mapper
def nameDon(x):
    parts = x.split("|")
    name = parts[7]
    city = parts[8]
    uniqueName = name + city
    try: donations = int(parts[14]) # Must be int, to use > and < on
    except: donations = 0
    return (uniqueName, donations)

# Just passing a reference to the function
# Automatically applied to each line of text
a = textData.map(nameDon)

# MAKE SET ONE
# x is the tuple yielded from the mapper
def overTenThous(x):
    donation = x[1]
    if donation >= 10000:
        return True
    else:
        return False

b = a.filter(overTenThous)

# now map that to name and distinct to make
# our set 1 of all those greater
setOne = b.map(lambda x: x[0]).distinct()

# now count set 1
setOneCount = setOne.count()


# NOW MAKE SET TWO
# Mapper - use function from before

# Just passing a reference to the function    
# Automatically applied to each line of text 
z  = textData.map(nameDon)

# Now filter to less than a thousand
def lessThan(x):
    donation = x[1]
    if donation <= 1000:
        return True
    else:
        return False

y = z.filter(lessThan)

# Now map to just name, and distinctify, to make our set2
setTwo = y.map(lambda x: x[0]).distinct()

# Find the intersection of set one and set two and count
both = setOne.intersection(setTwo).count()

# Total = Donated 100000 - donated both
result = setOneCount - both

print("Result = ", result)
