"""

This module loads the raw data, parses it and builds the following rdds 
1. postDf : It is a pySpark Dataframe, which contains all the parsed data from Posts.xml 
2. positive : It is a pySpark Dataframe, which contains all the instances in postDf, where we have a tag match .
3. negative : It is a pySpark Dataframe, which contains all the instances in postDf, where we dont have a tag match.
4. positiveTrain : It is a pySpark Dataframe, which contains a random sample from positive rdd(90% size).
5. negativeTrain : It is a pySpark Dataframe, which contains a random sample from negative rdd(90% size).
6. training : It is a pySpark Dataframe, build by a union operation of positiveTrain and negativeTrain rdds.
7. testing: It is a pySpark Dataframe, build by a subtraction operation of postDf and training rdds.

These rdds will be used by the ML module to build the actual classification model

"""


from pyspark.sql.types import *
import re

"""define a custom schema"""
customSchema = StructType([ \
    StructField("_id", StringType(), True), \
    StructField("tags", StringType(), True), \
    StructField("body", StringType(), True)])

postXml = sc.textFile("../data/Posts.xml")
          .map(lambda x:x.strip())
          .filter(lambda x:x.startswith("<?xml version=")==False)
          .filter(lambda x:x!="<posts>" and x!="</posts>")
          
xml = spark.createDataFrame(postXml.map(parse).filter(lambda x:x!=None), customSchema)


"""
Now we can use the xml rdd to build our model
"""

# Custum function to generete an output label
def sqlFunc(val):
    if(val.find("brew")>-1):
        return 1.0
    else:
        return 0.0

udfFunc = udf(sqlFunc, FloatType())
postDf = xml.withColumn("label",udfFunc("tags"))

# we build two rdds, positive contains the set of instances where we have a tag match, and negative contains the remaining instances
positive = postDf.filter(postDf.label>0.0)
negative = postDf.filter(postDf.label<1.0)

# we build the training set using 90% of the data and use the remaining 10% data for testing
postDf.registerTempTable('table_postDf')

# training
positiveTrain = positive.sample(False, 0.9)
negativeTrain = negative.sample(False, 0.9)
training = positiveTrain.unionAll(negativeTrain)

# testing
training.registerTempTable('table_training')
testing = sqlContext.sql('select p.* from table_postDf p left join table_training t on p._id = t._id where t._id is null')









