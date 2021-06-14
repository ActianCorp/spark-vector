FROM ubuntu:focal

ENV JAVA_VERSION 8
ENV SPARK_VERSION 3.1.1
ENV HADOOP_VERSION 3.2

# Install basics
RUN apt-get update && apt-get install -y apt-transport-https curl wget gnupg software-properties-common vim

# Install kubectl
RUN curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add && \
    apt-add-repository "deb http://apt.kubernetes.io/ kubernetes-xenial main" && \
    apt-get update && \
    apt-get install -y kubectl

# Install Java JRE
RUN apt-get install -y openjdk-${JAVA_VERSION}-jre

# Install Spark
RUN mkdir -p /spark && \
    cd /spark && \
    wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xfvz spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz --directory=/spark --strip 1 && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz