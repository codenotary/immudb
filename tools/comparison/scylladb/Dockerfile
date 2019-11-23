FROM scylladb/scylla
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
RUN python get-pip.py
RUN pip install cassandra-driver
ADD . .
ENTRYPOINT [ "/run.sh" ]
