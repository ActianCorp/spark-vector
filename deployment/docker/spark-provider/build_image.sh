#!/bin/bash

#REQUIREMENTS: curl, sbt and docker have to be on PATH
#ARG 1: Tag of the to-be-created docker image

set -e

SPARK_VERSION=3.1.1
HADOOP_VERSION=3.2
JAVA_VERSION=8
GCS_VERSION=2.1.5

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(echo $SCRIPT_DIR | sed 's/\/deployment.*//')"
PATH_TO_PROVIDER_DIR=$PROJECT_ROOT/provider/target

SPARK_URL=https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/
SPARK_FOLDER=spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
SPARK_PKG=tgz
JAVA_IMAGE=$JAVA_VERSION-jre-slim
# shaded version contains all dependencies
GCS_CONNECTOR_URL=https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-$GCS_VERSION
GCS_CONNECTOR_JAR=gcs-connector-hadoop3-$GCS_VERSION-shaded.jar

#Build the Spark Vector Provider jar file
cd $PROJECT_ROOT
sbt provider/assembly
cd $SCRIPT_DIR

#Download Spark distribution
curl $SPARK_URL$SPARK_FOLDER.$SPARK_PKG -o $SPARK_FOLDER.$SPARK_PKG
mkdir $SPARK_FOLDER
tar xfvz $SPARK_FOLDER.$SPARK_PKG --directory=$SPARK_FOLDER --strip 1
rm $SPARK_FOLDER.$SPARK_PKG

#Download GCS jar and add it to Spark's jar folder
curl $GCS_CONNECTOR_URL/$GCS_CONNECTOR_JAR -o ./$SPARK_FOLDER/jars/$GCS_CONNECTOR_JAR
chmod 644 ./$SPARK_FOLDER/jars/$GCS_CONNECTOR_JAR

#Add Spark Vector Provider to Spark's jar folder
if [[ -d "$PATH_TO_PROVIDER_DIR" ]]; then
	find $PATH_TO_PROVIDER_DIR -regex ".*spark_vector_provider.*.jar" -exec cp {} ./$SPARK_FOLDER/jars/spark_vector_provider.jar \;
fi

#Add additional jar files to Spark's jar folder
if [[ -d "jars" ]]; then
	find jars -name "*.jar" -exec cp {} ./$SPARK_FOLDER/jars/ \;
fi

#Build the final Docker image
cd $SPARK_FOLDER
sed -i '' -e "s/\(.*ARG java_image_tag=\).*/\1$JAVA_IMAGE/" kubernetes/dockerfiles/spark/Dockerfile
docker build -t $1 -f kubernetes/dockerfiles/spark/Dockerfile .
cd ..
rm -rf $SPARK_FOLDER