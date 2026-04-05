from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
import sys
import re
import math

query_text = " ".join(sys.argv[1:])
if not query_text.strip():
    print("Usage: query.py <query text>")
    sys.exit(1)

# tokenize query the same way as the indexer
terms = re.sub(r'[^a-z0-9\s]', '', query_text.lower()).split()
terms = list(set(terms))

if not terms:
    print("No valid terms in query")
    sys.exit(0)

print("Query: %s" % query_text)
print("Terms: %s" % str(terms))

# BM25 params
k1 = 1.5
b = 0.75

# read corpus stats and index entries from Cassandra
cluster_conn = Cluster(['cassandra-server'])
session = cluster_conn.connect('search_engine')

stats_row = session.execute("SELECT num_docs, avgdl FROM corpus_stats WHERE id = 1").one()
if stats_row is None:
    print("No corpus stats found. Did you run the indexer?")
    sys.exit(1)

N = stats_row.num_docs
avgdl = stats_row.avgdl

# get df for each query term
term_df = {}
for t in terms:
    row = session.execute("SELECT df FROM vocabulary WHERE term = %s", [t]).one()
    if row:
        term_df[t] = row.df

# gather all posting list entries for query terms
index_entries = []
for t in terms:
    if t not in term_df:
        continue
    rows = session.execute(
        "SELECT term, doc_id, doc_title, tf, dl FROM inverted_index WHERE term = %s", [t]
    )
    for r in rows:
        index_entries.append((r.term, r.doc_id, r.doc_title, r.tf, r.dl))

cluster_conn.shutdown()

if not index_entries:
    print("No results found for query: %s" % query_text)
    sys.exit(0)

# PySpark BM25 computation
spark = SparkSession.builder.appName("BM25Search").getOrCreate()
sc = spark.sparkContext

b_N = sc.broadcast(N)
b_avgdl = sc.broadcast(avgdl)
b_term_df = sc.broadcast(term_df)
b_k1 = sc.broadcast(k1)
b_b = sc.broadcast(b)

rdd = sc.parallelize(index_entries)

def calc_bm25(entry):
    term, doc_id, doc_title, tf, dl = entry
    df = b_term_df.value[term]
    idf = math.log(b_N.value / df)
    num = (b_k1.value + 1) * tf
    denom = b_k1.value * ((1 - b_b.value) + b_b.value * dl / b_avgdl.value) + tf
    score = idf * num / denom
    return (doc_id, (doc_title, score))

top10 = rdd.map(calc_bm25) \
    .reduceByKey(lambda a, x: (a[0], a[1] + x[1])) \
    .sortBy(lambda r: r[1][1], ascending=False) \
    .take(10)

print("")
print("Top 10 results for: %s" % query_text)
print("-" * 60)
for rank, (doc_id, (title, score)) in enumerate(top10, 1):
    print("%d. [Doc %s] %s (score: %.4f)" % (rank, doc_id, title, score))
print("-" * 60)

spark.stop()
