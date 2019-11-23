# sequential, non-concurrent inserts with configurable batch size

from cassandra.query import BatchStatement
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

iterations = 1000
batch_size = 10
print("Running", iterations, "iterations at batch size", batch_size)
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('ks_test')
stmt = session.prepare('INSERT INTO test (key,value) VALUES (?, ?)')
batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)
for k in range(iterations):
	for i,j in [(str(x), "123456789") for x in range(batch_size)]:
	    try:
	      batch.add(stmt,(i,j))
	    except Exception as e:
	      print('Cassandra error: {}'.format(e))
	session.execute(batch)
