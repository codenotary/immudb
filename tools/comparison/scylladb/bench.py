# sequential, non-concurrent batch inserts

from timeit import timeit
from cassandra.query import BatchStatement
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

ITERATIONS = 5000
BATCH_SIZE = 100
TXS = ITERATIONS * BATCH_SIZE

cluster = Cluster(["127.0.0.1"])
session = cluster.connect("ks_test")
stmt = session.prepare("INSERT INTO test (key,value) VALUES (?, ?)")

def run(iterations, batch_size):
	for k in range(iterations):
		batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)
		for i, j in [(str(x), "123456789") for x in range(batch_size)]:
			batch.add(stmt,(i,j))
		session.execute(batch)
		if k % 100 == 0:
			print("Transaction:\t{}".format(k * batch_size))

print("Running {} iterations at batch size {}".format(ITERATIONS, BATCH_SIZE))
duration = timeit(lambda: run(ITERATIONS, BATCH_SIZE), number=1)
print("================================================")
print("Duration:\t{}s".format(duration))
print("Transactions:\t{}".format(TXS))
print("Throughput:\t{} tx/s".format(TXS / duration))
