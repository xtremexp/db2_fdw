FROM builder

ENV DB2INST1_PASSWORD=testpwd
ENV DBNAME=test
ENV DB2_HOME=/opt/ibm/db2/V12.1
# The DB2 CLI refuses to work without DB2INSTANCE being set (SQL1390C), even
# for remote connections only.  A minimal local instance provides it.
ENV DB2INSTANCE=db2inst1
ENV DB2CLIINIPATH=/opt/ibm/db2/V12.1/cfg

COPY ./rpms/* /rpms/
COPY start.sh /

RUN dnf install -y /rpms/*.rpm && \
    mkdir /pgdata && \
    chown postgres: /pgdata && \
    useradd -m db2inst1 && \
    /opt/ibm/db2/V12.1/instance/db2icrt -s client -nosharedgroup db2inst1 && \
    chmod -R a+rX /home/db2inst1/sqllib && \
    # DSNs used by the regression tests; adjust host/port/database as needed
    printf '[SAMPLE]\nDatabase=SAMPLE\nProtocol=tcpip\nHostname=db2\nPort=50000\n\n[SAMPLE1]\nDatabase=SAMPLE\nProtocol=tcpip\nHostname=db2\nPort=50000\n' \
      > /opt/ibm/db2/V12.1/cfg/db2cli.ini

USER	postgres

CMD   ./start.sh
