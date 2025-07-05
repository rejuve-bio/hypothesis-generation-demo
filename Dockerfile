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
    software-properties-common \
    dirmngr \
    gpg-agent \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install R 4.5 from source
RUN apt-get update && \
    apt-get install -y \
        libreadline-dev \
        libx11-dev \
        libxt-dev \
        libpcre2-dev \
        libbz2-dev \
        liblzma-dev \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

# Download and compile R 4.5
RUN cd /tmp && \
    wget https://cran.r-project.org/src/base/R-4/R-4.5.0.tar.gz && \
    tar -xzf R-4.5.0.tar.gz && \
    cd R-4.5.0 && \
    ./configure --enable-R-shlib --with-blas --with-lapack && \
    make && \
    make install && \
    cd / && \
    rm -rf /tmp/R-4.5.0*

# Install comprehensive system dependencies for R packages
RUN apt-get update && \
    apt-get install -y \
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
        libmagick++-dev \
        libudunits2-dev \
        libgdal-dev \
        libproj-dev \
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

# Install plink2
RUN mkdir -p /opt/plink2 && \
    wget -q https://s3.amazonaws.com/plink2-assets/alpha6/plink2_linux_x86_64_20250701.zip -O /tmp/plink2.zip && \
    unzip -q /tmp/plink2.zip -d /opt/plink2 && \
    find /opt/plink2 -name "plink2" -type f -exec chmod +x {} \; && \
    find /opt/plink2 -name "plink2" -type f -exec ln -sf {} /usr/local/bin/plink2 \; && \
    rm /tmp/plink2.zip

# Install GCTA64
RUN mkdir -p /opt/gcta && \
    wget -q https://yanglab.westlake.edu.cn/software/gcta/bin/gcta-1.94.4-linux-kernel-3-x86_64.zip -O /tmp/gcta.zip && \
    unzip -q /tmp/gcta.zip -d /opt/gcta && \
    find /opt/gcta -name "gcta64" -type f -exec chmod +x {} \; && \
    find /opt/gcta -name "gcta64" -type f -exec ln -sf {} /usr/local/bin/gcta64 \; && \
    rm /tmp/gcta.zip

# Upgrade pip and install Python build tools
RUN pip install --upgrade pip setuptools wheel

# Install the latest BiocManager and set up Bioconductor
RUN Rscript -e " \
    install.packages('BiocManager', repos='https://cloud.r-project.org/'); \
    BiocManager::install(version='3.21', ask=FALSE); \
    "

# Install basic R packages
RUN Rscript -e " \
    options(repos = c(CRAN = 'https://cloud.r-project.org/')); \
    options(Ncpus = parallel::detectCores()); \
    BiocManager::install(c('dplyr', 'readr', 'data.table', 'Rfast', 'devtools'), ask=FALSE, update=TRUE, force=TRUE) \
    "

# Install BiocManager packages
RUN Rscript -e " \
    BiocManager::install(c('susieR'), ask=FALSE, update=TRUE, force=TRUE); \
    "

# Install the latest MungeSumstats from Bioconductor
RUN Rscript -e " \
    BiocManager::install(c('MungeSumstats', 'SNPlocs.Hsapiens.dbSNP155.GRCh37', 'SNPlocs.Hsapiens.dbSNP155.GRCh38'), ask=FALSE, update=TRUE, force=TRUE); \
    "

# Install required SNPlocs packages for reference genome support
RUN Rscript -e " \
    BiocManager::install(c( \
        'SNPlocs.Hsapiens.dbSNP155.GRCh37', \
        'SNPlocs.Hsapiens.dbSNP155.GRCh38', \
        'BSgenome.Hsapiens.UCSC.hg19', \
        'BSgenome.Hsapiens.UCSC.hg38', \
        'BSgenome.Hsapiens.1000genomes.hs37d5' \
    ), ask=FALSE, update=TRUE, force=TRUE); \
    "

# Install vautils from GitHub
RUN Rscript -e " \
    library(devtools); \
    withr::with_envvar(c('GITHUB_PAT' = ''), { \
        install_github('oyhel/vautils', upgrade='never') \
    }) \
    "

# Install the Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

# Install rpy2
RUN pip install rpy2

# Copy the current directory contents into the container
COPY . /app

# Expose port 5000 for Flask
EXPOSE 5000