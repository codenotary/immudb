# sequential, non-concurrent inserts with configurable batch size

import timeit
from cassandra.query import BatchStatement
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('ks_test')
stmt = session.prepare('INSERT INTO test (key,value) VALUES (?, ?)')
batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)

def run(iterations, batch_size):
	print("Running", iterations, "iterations at batch size", batch_size)
	for k in range(iterations):
		for i,j in [(str(x), "123456789") for x in range(batch_size)]:
			try:
			  batch.add(stmt,(i,j))
			except Exception as e:
			  print('Cassandra error: {}'.format(e))
		session.execute(batch)
		if k % 100 == 0:
			print("Checkpoint", k * batch_size)

duration = timeit.timeit(lambda: run(1000, 10), number=1)
print(duration, "s", )
print("Statements", 1000 * 10)
print("Throughput", 1000 * 10 / duration)
