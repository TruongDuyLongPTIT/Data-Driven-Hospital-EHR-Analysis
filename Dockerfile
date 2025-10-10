FROM flink:1.19.1-scala_2.12
RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip3 install boto3 pyarrow apache-flink
