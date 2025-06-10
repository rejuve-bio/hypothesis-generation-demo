# Use an official Python runtime as a parent image
# FROM python:3.10
FROM base-image:latest


# Install Rust
RUN apt-get update && apt-get install -y build-essential curl

# Install the outlines library
# RUN pip install --upgrade pip setuptools wheel

# Install rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

# # Set the working directory in the container
# WORKDIR /app

# Copy the current directory contents into the container
COPY . /app
# Expose port 5000 for Flask
EXPOSE 5000
