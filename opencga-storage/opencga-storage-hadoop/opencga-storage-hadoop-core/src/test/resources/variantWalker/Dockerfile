# Use an official Python runtime as a parent image
FROM python:3.8-slim-buster

# Set the working directory in the container to /app
WORKDIR /app

ARG PYTHON_PATH="."

LABEL storage="do_not_delete"

RUN echo ${PYTHON_PATH}
# Copy the python directory contents into the container at /app
COPY ${PYTHON_PATH} /app

# Install any needed packages specified in requirements.txt
RUN ls -la /app
RUN pip install --no-cache-dir -r requirements.txt
