#!/usr/bin/env python3
import subprocess,sys,logging, time, string, os
import xml.etree.ElementTree as ET
import xml.sax.saxutils as saxutils
import influxdb_client

TAG="bla"
logging.basicConfig(
	format='%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s', level=logging.INFO
	)

def build_docker():
	logging.info("Building docker")
	t0=time.time()
	result=subprocess.run(
		["docker", "build", "-f", "Dockerfile", "../..", "-t", TAG],
		capture_output=True, text=True
		)
	if result.returncode!=0:
		logging.error("Docker build failure:", result.stderr)
		return False, time.time()-t0
	return True, time.time()-t0

def cleanup(s):
	ret=""
	for c in s:
		if c in string.printable:
			ret=ret+c
	return saxutils.escape(ret)

def replication(ts):
	logging.info("Starting replication test")
	xmlresult = ET.SubElement(ts, 'testcase', name="replication")
	t0=time.time()
	result=subprocess.run(
		["docker", "run", "--tty", "--rm", "--entrypoint", "/src/immudb/test/e2e/replication/run.sh", TAG],
		capture_output=True, text=True
		)
	for l in result.stdout.split("\n")[-5:]:
		logging.info("replication result: %s", l)
	ET.SubElement(xmlresult, "system-out").text=cleanup(result.stdout)
	ET.SubElement(xmlresult, "system-err").text=cleanup(result.stderr)
	xmlresult.set("time", str(time.time()-t0))
	if result.returncode!=0:
		logging.error("Docker replication test:", result.stderr)
		xmlresult.set("status", "failure")
		ET.SubElement(xmlresult, "failure", message="Test failed")
		return False
	xmlresult.set("status", "success")
	return True

def truncation(ts):
	logging.info("Starting truncation test")
	xmlresult = ET.SubElement(ts, 'testcase', name="truncation")
	t0=time.time()

	result=subprocess.run(
		["docker", "run", "--tty", "--rm", "--entrypoint", "/src/immudb/test/e2e/truncation/run.sh", TAG],
		capture_output=True, text=True
		)
	ET.SubElement(xmlresult, "system-out").text=cleanup(result.stdout)
	ET.SubElement(xmlresult, "system-err").text=cleanup(result.stderr)
	xmlresult.set("time", str(time.time()-t0))

	if result.returncode!=0:
		logging.error("Docker truncation test: %s", result.stderr)
		xmlresult.set("status", "failure")
		ET.SubElement(xmlresult, "failure", message="Test failed")
	else:
		xmlresult.set("status", "success")
	ret=False
	for l in result.stdout.split("\n")[-5:]:
		if "OK:" in l:
			ret=True
		logging.info("truncation result: %s", l)
	return ret

if not build_docker():
	sys.exit(1)

root=ET.Element('testsuites')
tree=ET.ElementTree(root)
ts = ET.SubElement(root, 'testsuite', name="e2e")
err=0
for test in (replication, truncation):
	if not test(ts):
		err=err+1
ts.set("errors",str(err))
ET.dump(tree)
tree.write("result.xml")

if all(map(os.getenv,["INFLUX_TOKEN", "INFLUX_ORG", "INFLUX_BUCKET", "INFLUX_ORG"])):
	# here all env variable are set
	logging.info("Sending data to influxdb")
	org=os.getenv("INFLUX_ORG")
	token=os.getenv("INFLUX_TOKEN")
	url=os.getenv("INFLUX_URL")
	bucket=os.getenv("INFLUX_BUCKET")
	jobname=os.getenv("JOB_NAME","none")
	client = influxdb_client.InfluxDBClient(url=url, token=token, org=org )
	repl_time=float(tree.findall('./testsuite/testcase[@name="replication"]')[0].attrib['time'])
	repl_status=tree.findall('./testsuite/testcase[@name="replication"]')[0].attrib['status']
	trunc_time=float(tree.findall('./testsuite/testcase[@name="truncation"]')[0].attrib['time'])
	trunc_status=tree.findall('./testsuite/testcase[@name="truncation"]')[0].attrib['status']
	p = influxdb_client.Point("immudb-replication-truncation") \
		.tag("job", jobname) \
		.field("replication_time", repl_time) \
		.field("replication_status", repl_status) \
		.field("truncation_time", trunc_time) \
		.field("truncation_status", trunc_status)
	with client.write_api(
		write_options=influxdb_client.client.write_api.SYNCHRONOUS
		) as write_api:
		write_api.write(bucket=bucket, org=org, record=p)
	logging.info("Sent")

