from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.ml.feature import HashingTF, IDF, Tokenizer

numFeatures = 64000
numEpochs = 30
regParam = 0.02

tokenizer=Tokenizer(inputCol="body", outputCol="Words")
hashingTf=HashingTF(inputCol="Words", outputCol="Features", numFeatures=numFeatures);

lr = LogisticRegression(maxIter=numEpochs, regParam=regParam, elasticNetParam=0.8)
    .setFeaturesCol("Features")
    .setLabelCol("label")
    .setRawPredictionCol("Score")
    .setPredictionCol("Prediction")

pipeline = Pipeline().setStages([tokenizer, hashingTF, lr])
pipeline.fit(training)

testResult=model.transform(testing)
testingResultScores=testResult.select("Prediction", "Label").rdd.map(lambda x:(float(x[0]), float(x[1])))

print(BinaryClassificationMetrics(testingResultScores).areaUnderROC)
