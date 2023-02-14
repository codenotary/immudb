#!/usr/bin/env python3
import subprocess,sys,logging, time
import xml.etree.ElementTree as ET
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
	ET.SubElement(xmlresult, "system-out").text=result.stdout
	ET.SubElement(xmlresult, "system-err").text=result.stderr
	xmlresult.set("time", str(time.time()-t0))
	if result.returncode!=0:
		logging.error("Docker replication test:", result.stderr)
		return False
	return True

def truncation(ts):
	logging.info("Starting truncation test")
	xmlresult = ET.SubElement(ts, 'testcase', name="truncation")
	t0=time.time()

	result=subprocess.run(
		["docker", "run", "--tty", "--rm", "--entrypoint", "/src/immudb/test/e2e/truncation/run.sh", TAG],
		capture_output=True, text=True
		)
	ET.SubElement(xmlresult, "system-out").text=result.stdout
	ET.SubElement(xmlresult, "system-err").text=result.stderr
	xmlresult.set("time", str(time.time()-t0))

	if result.returncode!=0:
		logging.error("Docker truncation test: %s", result.stderr)

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

