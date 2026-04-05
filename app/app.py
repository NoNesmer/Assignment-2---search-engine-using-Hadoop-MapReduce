from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
import time
import sys


def wait_for_cassandra(host, retries=30, delay=10):
    for i in range(retries):
        try:
            c = Cluster([host])
            s = c.connect()
            print("Connected to Cassandra")
            return c, s
        except Exception as e:
            print("Waiting for Cassandra (%d/%d)... %s" % (i + 1, retries, str(e)))
            time.sleep(delay)
    print("Could not connect to Cassandra after %d retries" % retries)
    sys.exit(1)


cluster, session = wait_for_cassandra('cassandra-server')

# create keyspace
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_engine
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
session.set_keyspace('search_engine')

# create tables
session.execute("""
    CREATE TABLE IF NOT EXISTS inverted_index (
        term text,
        doc_id text,
        doc_title text,
        tf int,
        dl int,
        PRIMARY KEY (term, doc_id)
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS vocabulary (
        term text PRIMARY KEY,
        df int
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS doc_stats (
        doc_id text PRIMARY KEY,
        doc_title text,
        dl int
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS corpus_stats (
        id int PRIMARY KEY,
        num_docs int,
        avgdl double
    )
""")

print("Tables created.")

# read data from HDFS using PySpark
spark = SparkSession.builder \
    .appName("StoreIndex") \
    .master("local") \
    .getOrCreate()

sc = spark.sparkContext

# load inverted index: term \t doc_id \t doc_title \t tf \t dl
index_rdd = sc.textFile("/indexer/index")
index_data = index_rdd.map(lambda line: line.split('\t')).collect()

# load vocabulary: term \t df
vocab_rdd = sc.textFile("/indexer/vocab")
vocab_data = vocab_rdd.map(lambda line: line.split('\t')).collect()

spark.stop()

print("Read %d index entries and %d vocab entries from HDFS" % (len(index_data), len(vocab_data)))

# insert inverted index and extract doc stats along the way
insert_idx = session.prepare(
    "INSERT INTO inverted_index (term, doc_id, doc_title, tf, dl) VALUES (?, ?, ?, ?, ?)"
)

doc_info = {}
for row in index_data:
    if len(row) < 5:
        continue
    term, doc_id, doc_title = row[0], row[1], row[2]
    tf, dl = int(row[3]), int(row[4])
    session.execute(insert_idx, [term, doc_id, doc_title, tf, dl])
    if doc_id not in doc_info:
        doc_info[doc_id] = (doc_title, dl)

print("Inserted inverted index (%d entries)" % len(index_data))

# insert vocabulary
insert_voc = session.prepare("INSERT INTO vocabulary (term, df) VALUES (?, ?)")
for row in vocab_data:
    if len(row) < 2:
        continue
    session.execute(insert_voc, [row[0], int(row[1])])

print("Inserted vocabulary (%d terms)" % len(vocab_data))

# insert doc stats
insert_doc = session.prepare("INSERT INTO doc_stats (doc_id, doc_title, dl) VALUES (?, ?, ?)")
total_dl = 0
for doc_id, (doc_title, dl) in doc_info.items():
    session.execute(insert_doc, [doc_id, doc_title, dl])
    total_dl += dl

N = len(doc_info)
avgdl = total_dl / N if N > 0 else 0.0

# insert corpus-level stats
session.execute(
    "INSERT INTO corpus_stats (id, num_docs, avgdl) VALUES (1, %s, %s)",
    [N, avgdl]
)

print("=" * 50)
print("Indexing summary:")
print("  Documents: %d" % N)
print("  Vocabulary size: %d" % len(vocab_data))
print("  Avg document length: %.2f" % avgdl)
print("=" * 50)

cluster.shutdown()
