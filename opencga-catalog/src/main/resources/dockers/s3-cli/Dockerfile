# Simple Docker to run AWS clis
FROM ubuntu:18.04

RUN apt-get update && apt-get install -y curl unzip groff

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"

RUN unzip awscliv2.zip

RUN ./aws/install -i ~/.local/aws-cli -b ~/.local/bin/aws
ENV PATH="/root/.local/bin/aws:$PATH"

# To run anything on S3, we will do something like:
# sudo docker run aws /bin/bash -c "aws configure set aws_access_key_id {key_id} && aws configure set aws_secret_access_key {secretKey} && aws s3 s3://xxx-bucket/thesis.pdf -" > thesis.pdf