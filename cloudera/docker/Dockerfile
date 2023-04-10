FROM docker-private.infra.cloudera.com/cloudera_base/ubuntu:20.04

RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        openjdk-11-jdk \
        bash \
        krb5-admin-server \
        krb5-kdc \
        krb5-config

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
RUN mkdir -p /opt/ozone-addon/cli/ /cdep /opt/ozone-addon/jar/ /ozone-parcel/ozone-fatjar/

COPY . /opt/ozone-addon/cli/
RUN mv /opt/ozone-addon/cli/ozone-filesystem-hadoop3*.jar /opt/ozone-addon/jar/
RUN ln -s /opt/ozone-addon/jar/ozone-filesystem-hadoop3*.jar /ozone-parcel/ozone-fatjar/$(basename /opt/ozone-addon/jar/ozone-filesystem-hadoop3*.jar)