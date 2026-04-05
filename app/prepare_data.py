from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .getOrCreate()

sc = spark.sparkContext

# read all text docs - try HDFS first, fall back to local
try:
    files_rdd = sc.wholeTextFiles("hdfs:///data/")
    files_rdd = files_rdd.filter(lambda x: x[0].endswith('.txt'))
    cnt = files_rdd.count()
    if cnt < 100:
        # some files may have failed HDFS upload due to encoding
        # read from local instead
        print("HDFS has %d files, reading from local filesystem instead" % cnt)
        files_rdd = sc.wholeTextFiles("file:///app/data/")
        files_rdd = files_rdd.filter(lambda x: x[0].endswith('.txt'))
except:
    files_rdd = sc.wholeTextFiles("file:///app/data/")
    files_rdd = files_rdd.filter(lambda x: x[0].endswith('.txt'))

def parse_doc(file_tuple):
    filepath, content = file_tuple
    filename = filepath.split('/')[-1]
    name = filename.replace('.txt', '')
    # filename format: <doc_id>_<doc_title>.txt
    parts = name.split('_', 1)
    doc_id = parts[0]
    doc_title = parts[1].replace('_', ' ') if len(parts) > 1 else ""
    # replace tabs and newlines so we can use tab-separated format
    clean = content.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    return doc_id + '\t' + doc_title + '\t' + clean

docs_rdd = files_rdd.map(parse_doc).coalesce(1)
docs_rdd.saveAsTextFile("/input/data")

cnt = docs_rdd.count()
print("Prepared %d documents for indexing" % cnt)

spark.stop()
