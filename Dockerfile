# Use an official Python runtime as a parent image
FROM python:3.10

# Note: Using standard Debian R packages for compatibility
# We'll get the latest available versions that work together

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

# Install R and comprehensive system dependencies using standard Debian packages
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

# Install plink (plink1)
RUN mkdir -p /opt/plink && \
    wget -q https://s3.amazonaws.com/plink1-assets/plink_linux_x86_64_20231211.zip -O /tmp/plink.zip && \
    unzip -q /tmp/plink.zip -d /opt/plink && \
    chmod +x /opt/plink/plink && \
    ln -s /opt/plink/plink /usr/local/bin/plink && \
    rm /tmp/plink.zip

# Install plink2 (required for fine-mapping LD calculations)
RUN mkdir -p /opt/plink2 && \
    wget -q https://s3.amazonaws.com/plink2-assets/alpha6/plink2_linux_x86_64_20250701.zip -O /tmp/plink2.zip && \
    unzip -q /tmp/plink2.zip -d /opt/plink2 && \
    find /opt/plink2 -name "plink2" -type f -exec chmod +x {} \; && \
    find /opt/plink2 -name "plink2" -type f -exec ln -sf {} /usr/local/bin/plink2 \; && \
    rm /tmp/plink2.zip

# Install GCTA64 (needed for COJO analysis)
RUN mkdir -p /opt/gcta && \
    wget -q https://yanglab.westlake.edu.cn/software/gcta/bin/gcta-1.94.4-linux-kernel-3-x86_64.zip -O /tmp/gcta.zip && \
    unzip -q /tmp/gcta.zip -d /opt/gcta && \
    find /opt/gcta -name "gcta64" -type f -exec chmod +x {} \; && \
    find /opt/gcta -name "gcta64" -type f -exec ln -sf {} /usr/local/bin/gcta64 \; && \
    rm /tmp/gcta.zip

# Upgrade pip and install Python build tools
RUN pip install --upgrade pip setuptools wheel

# Install R packages - start with BiocManager and get the latest compatible version
RUN Rscript -e " \
    install.packages('BiocManager', repos='https://cloud.r-project.org/'); \
    BiocManager::install(); \
    "

# Install basic R packages
RUN Rscript -e " \
    options(repos = c(CRAN = 'https://cloud.r-project.org/')); \
    options(Ncpus = parallel::detectCores()); \
    BiocManager::install(c('dplyr', 'readr', 'data.table', 'Rfast', 'devtools'), ask=FALSE, update=TRUE) \
    "

# Install BiocManager packages - get the latest available MungeSumstats
RUN Rscript -e " \
    BiocManager::install(c('susieR', 'MungeSumstats'), ask=FALSE, update=TRUE, force=TRUE); \
    "

RUN Rscript -e " \
    library(devtools); \
    withr::with_envvar(c('GITHUB_PAT' = ''), { \
        install_github('oyhel/vautils', upgrade='never') \
    }) \
    "

# Verify the versions we got
RUN Rscript -e " \
    cat('=== VERSION VERIFICATION ===\n'); \
    cat('R version:', R.version.string, '\n'); \
    cat('Bioconductor version:', as.character(BiocManager::version()), '\n'); \
    cat('MungeSumstats version:', as.character(packageVersion('MungeSumstats')), '\n'); \
    cat('==============================\n'); \
    # Check if drop_indels parameter exists \
    help_text <- try(capture.output(help('format_sumstats', package='MungeSumstats')), silent=TRUE); \
    if (any(grepl('drop_indels', help_text))) { \
        cat('✓ drop_indels parameter is available\n'); \
    } else { \
        cat('✗ drop_indels parameter not found\n'); \
    } \
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