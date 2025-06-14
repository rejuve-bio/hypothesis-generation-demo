# Use an official Python runtime as a parent image
FROM python:3.10

# Install system dependencies in one layer with proper cleanup
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    wget \
    unzip \
    libgsl-dev \
    libblas-dev \
    liblapack-dev \
    gfortran \
    libc6-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install R and comprehensive system dependencies
RUN apt-get update && \
    apt-get install -y \
        r-base \
        r-base-dev \
        libcurl4-openssl-dev \
        libssl-dev \
        libxml2-dev \
        libfontconfig1-dev \
        libcairo2-dev \
        libharfbuzz-dev \
        libfribidi-dev \
        libfreetype6-dev \
        libpng-dev \
        libtiff5-dev \
        libjpeg-dev \
        libgit2-dev \
        libssh2-1-dev \
        zlib1g-dev \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Install plink
RUN mkdir -p /opt/plink && \
    wget -q https://s3.amazonaws.com/plink1-assets/plink_linux_x86_64_20231211.zip -O /tmp/plink.zip && \
    unzip -q /tmp/plink.zip -d /opt/plink && \
    chmod +x /opt/plink/plink && \
    ln -s /opt/plink/plink /usr/local/bin/plink && \
    rm /tmp/plink.zip

# Upgrade pip and install Python build tools
RUN pip install --upgrade pip setuptools wheel

# Install R packages step by step with better error handling
RUN Rscript -e " \
    options(repos = c(CRAN = 'https://cloud.r-project.org/')); \
    options(Ncpus = parallel::detectCores()); \
    install.packages(c('BiocManager', 'dplyr', 'readr', 'data.table'), dependencies=TRUE) \
    "

RUN Rscript -e " \
    options(repos = c(CRAN = 'https://cloud.r-project.org/')); \
    options(Ncpus = parallel::detectCores()); \
    install.packages('devtools', dependencies=TRUE) \
    "

RUN Rscript -e " \
    library(BiocManager); \
    BiocManager::install('susieR', ask=FALSE, update=FALSE, force=TRUE) \
    "

RUN Rscript -e " \
    library(devtools); \
    withr::with_envvar(c('GITHUB_PAT' = ''), { \
        install_github('oyhel/vautils', upgrade='never') \
    }) \
    "

# Install the Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

# Install rpy2 if not in requirements
RUN pip install rpy2

# Copy the current directory contents into the container
COPY . /app

# Expose port 5000 for Flask
EXPOSE 5000