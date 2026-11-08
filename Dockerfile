FROM eclipse-temurin:11-jre
ARG HBASE_VERSION=2.5.15
RUN apt-get update && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && f=hbase-${HBASE_VERSION}-bin.tar.gz \
 && (curl -fsSL https://dlcdn.apache.org/hbase/${HBASE_VERSION}/$f -o /tmp/$f \
     || curl -fsSL https://archive.apache.org/dist/hbase/${HBASE_VERSION}/$f -o /tmp/$f) \
 && tar -xzf /tmp/$f -C /opt && mv /opt/hbase-${HBASE_VERSION} /opt/hbase \
 && rm -rf /tmp/$f /opt/hbase/docs
ENV HBASE_HOME=/opt/hbase PATH=/opt/hbase/bin:$PATH
RUN printf '%s\n' '<configuration>' \
  '<property><name>hbase.rootdir</name><value>file:///data/hbase</value></property>' \
  '<property><name>hbase.zookeeper.property.dataDir</name><value>/data/zookeeper</value></property>' \
  '<property><name>hbase.unsafe.stream.capability.enforce</name><value>false</value></property>' \
  '<property><name>hbase.hfile.compaction.discharger.interval</name><value>10000</value></property>' \
  '</configuration>' > /opt/hbase/conf/hbase-site.xml
EXPOSE 16010 9090
WORKDIR /opt/hbase
CMD ["bash", "-c", "trap 'hbase-daemon.sh stop thrift; stop-hbase.sh; exit 0' TERM; start-hbase.sh && hbase-daemon.sh start thrift && tail -F logs/*.log & wait"]