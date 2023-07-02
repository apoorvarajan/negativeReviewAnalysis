from sparknlp.pretrained import PretrainedPipeline
import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
sparknlp.start()
# explain_document_pipeline = PretrainedPipeline("explain_document_ml")
# annotations = explain_document_pipeline.annotate("We are very happy about SparkNLP")
# print(annotations)
documentAssembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

tokenizer = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")

lemmatizer = Lemmatizer() \
    .setInputCols(["token"]) \
    .setOutputCol("lemma") \
    .setDictionary("lemmas_small.txt", "->", "\t")

sentimentDetector = SentimentDetector() \
    .setInputCols(["lemma", "document"]) \
    .setOutputCol("sentimentScore") \
    .setDictionary("default-sentiment-dict.txt", ",", ReadAs.TEXT)

pipeline = Pipeline().setStages([
    documentAssembler,
    tokenizer,
    lemmatizer,
    sentimentDetector,
])

data = spark.createDataFrame([
    ["The staff of the restaurant is nice"],
    ["I recommend others to avoid because it is too expensive"]
]).toDF("text")
result = pipeline.fit(data).transform(data)

result.selectExpr("sentimentScore.result").show(truncate=False)
