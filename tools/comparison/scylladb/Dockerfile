FROM scylladb/scylla
ADD . .
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
RUN python get-pip.py
RUN pip install cassandra-driver
ENTRYPOINT [ "/run.sh" ]
