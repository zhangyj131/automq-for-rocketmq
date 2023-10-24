FROM amazoncorretto:21 as build
WORKDIR /root
RUN mkdir -p /root/.m2 \
    && mkdir -p /root/repo \
    && mkdir -p /root/maven
ADD distribution/docker/settings.xml /root/.m2
COPY . /root/repo
RUN yum install wget tar xz gzip -y  \
    && wget -O /root/maven.tar.gz https://dlcdn.apache.org/maven/maven-3/3.9.5/binaries/apache-maven-3.9.5-bin.tar.gz \
    && tar -xzf /root/maven.tar.gz --strip-components=1 -C /root/maven \
    && cd /root/repo \
    && /root/maven/bin/mvn package -Dmaven.test.skip=true

FROM amazoncorretto:21
WORKDIR /root
RUN yum install which bash -y
COPY --from=build /root/repo/distribution/target/rocketmq-on-s3.tar.gz .
RUN tar -xzf rocketmq-on-s3.tar.gz && rm -f rocketmq-on-s3.tar.gz
CMD ["/root/rocketmq-on-s3/bin/start-broker.sh"]