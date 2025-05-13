# Use an official Python runtime as a parent image
FROM python:3.10


# Install Rust
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    wget \
    unzip \
    libgsl-dev \  
    libblas-dev \ 
    liblapack-dev 

# Install the outlines library
RUN pip install --upgrade pip setuptools wheel

# Install rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

# Set the working directory in the container
WORKDIR /app


# Install plink
RUN mkdir -p /opt/plink && \
    wget -q https://s3.amazonaws.com/plink1-assets/plink_linux_x86_64_20231211.zip -O /tmp/plink.zip && \
    unzip -q /tmp/plink.zip -d /opt/plink && \
    chmod +x /opt/plink/plink && \
    ln -s /opt/plink/plink /usr/local/bin/plink && \
    rm /tmp/plink.zip

# Install R and necessary system dependencies
RUN apt-get update && \
    apt-get install -y r-base r-base-dev libcurl4-openssl-dev libssl-dev libxml2-dev && \
    apt-get clean

# Install required R packages
RUN Rscript -e "install.packages(c('BiocManager', 'dplyr', 'readr', 'data.table', 'devtools'), repos='https://cran.rstudio.com/', dependencies=TRUE)" && \
    Rscript -e "BiocManager::install('susieR', ask=FALSE, update=FALSE)" && \
    Rscript -e "devtools::install_github('oyhel/vautils')"
    
# Install the Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

# Make sure rpy2 is installed
RUN pip install rpy2

# Copy the current directory contents into the container
COPY . /app
# Expose port 5000 for Flask
EXPOSE 5000
