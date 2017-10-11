
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
positive=postDf.filter(postDf.label>0.0)
negative=postDf.filter(postDf.label<1.0)

