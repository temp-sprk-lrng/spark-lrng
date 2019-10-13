FROM bde2020/spark-scala-template

ENV SPARK_APPLICATION_MAIN_CLASS com.example.App

ARG aws_bucket
ARG aws_secret_key
ARG aws_access_key
ENV SPARK_APPLICATION_ARGS "$aws_bucket  $aws_access_key $aws_secret_key"
