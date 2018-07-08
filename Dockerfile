FROM cassandra
FROM spotify/kafka
FROM openjdk:8

ENV SCALA_VERSION 2.12.6
ENV SBT_VERSION 1.1.6
ENV KAFKA_VERSION 0.10.2.0

RUN touch /usr/lib/jvm/java-8-openjdk-amd64/release
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils

RUN set -e; \
  curl -fsL https://downloads.typesafe.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.tgz | tar xfz - -C /root/ && \
    echo >> /root/.bashrc && \
      echo "export PATH=~/scala-$SCALA_VERSION/bin:$PATH" >> /root/.bashrc

RUN set -e; \
  curl -L -o sbt-$SBT_VERSION.deb https://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
    dpkg -i sbt-$SBT_VERSION.deb && \
      rm sbt-$SBT_VERSION.deb && \
        apt-get update && \
          apt-get install -y sbt && \
            sbt sbtVersion

ADD . /home/git
WORKDIR /home/git

CMD sbt run
