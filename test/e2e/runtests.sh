#!/bin/sh

test -f bin/activate || python3 -mvenv .
./bin/pip install -qr requirements.txt
./bin/python3 runtests.py
