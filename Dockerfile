#  For susie 0.12.35
#  Use an official Python runtime as a parent image
FROM python:3.10

# Install all system dependencies in one layer
RUN apt-get update && apt-get install -y \
    build-essential curl wget unzip software-properties-common dirmngr gpg-agent \
    libgsl-dev libblas-dev liblapack-dev gfortran libc6-dev \
    libreadline-dev libx11-dev libxt-dev libpcre2-dev libbz2-dev liblzma-dev \
    libcurl4-openssl-dev libssl-dev libxml2-dev libfontconfig1-dev \
    libcairo2-dev libharfbuzz-dev libfribidi-dev libfreetype6-dev \
    libpng-dev libtiff5-dev libjpeg-dev libgit2-dev libssh2-1-dev \
    zlib1g-dev libmagick++-dev libudunits2-dev libgdal-dev libproj-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install R 4.4.2 from source
RUN cd /tmp && \
    wget https://cran.r-project.org/src/base/R-4/R-4.4.2.tar.gz && \
    tar -xzf R-4.4.2.tar.gz && \
    cd R-4.4.2 && \
    ./configure --enable-R-shlib --with-blas --with-lapack && \
    make && make install && \
    cd / && rm -rf /tmp/R-4.4.2*

WORKDIR /app

# Install genomics tools
RUN mkdir -p /opt/plink && \
    wget -q https://s3.amazonaws.com/plink1-assets/plink_linux_x86_64_20231211.zip -O /tmp/plink.zip && \
    unzip -q /tmp/plink.zip -d /opt/plink && \
    chmod +x /opt/plink/plink && \
    ln -s /opt/plink/plink /usr/local/bin/plink && \
    rm /tmp/plink.zip

RUN mkdir -p /opt/plink2 && \
    wget -q https://s3.amazonaws.com/plink2-assets/alpha6/plink2_linux_x86_64_20250701.zip -O /tmp/plink2.zip && \
    unzip -q /tmp/plink2.zip -d /opt/plink2 && \
    find /opt/plink2 -name "plink2" -type f -exec chmod +x {} \; && \
    find /opt/plink2 -name "plink2" -type f -exec ln -sf {} /usr/local/bin/plink2 \; && \
    rm /tmp/plink2.zip

RUN mkdir -p /opt/gcta && \
    wget -q https://yanglab.westlake.edu.cn/software/gcta/bin/gcta-1.94.4-linux-kernel-3-x86_64.zip -O /tmp/gcta.zip && \
    unzip -q /tmp/gcta.zip -d /opt/gcta && \
    find /opt/gcta -name "gcta64" -type f -exec chmod +x {} \; && \
    find /opt/gcta -name "gcta64" -type f -exec ln -sf {} /usr/local/bin/gcta64 \; && \
    rm /tmp/gcta.zip

RUN pip install --upgrade pip setuptools wheel

# Install BiocManager first
RUN Rscript -e " \
    options(repos = c(CRAN = 'https://cloud.r-project.org/')); \
    install.packages('remotes'); \
    remotes::install_version('BiocManager', version = '1.30.25', repos = 'https://cloud.r-project.org/', upgrade = 'never'); \
    bio_version <- packageVersion('BiocManager'); \
    cat('BiocManager version:', as.character(bio_version), '\n'); \
    BiocManager::install(version='3.20', ask=FALSE, update=FALSE); \
    "

# Install susieR 0.12.35 specifically
RUN Rscript -e " \
    options(repos = c(CRAN = 'https://cloud.r-project.org/')); \
    cat('=== INSTALLING susieR 0.12.35 ===\n'); \
    \
    # Download specific version from CRAN archive \
    cat('Downloading susieR 0.12.35 from CRAN archive...\n'); \
    download.file('https://cran.r-project.org/src/contrib/Archive/susieR/susieR_0.12.35.tar.gz', '/tmp/susieR_0.12.35.tar.gz'); \
    \
    # Install dependencies first \
    cat('Installing dependencies...\n'); \
    install.packages(c('mixsqp', 'reshape', 'ggplot2', 'Matrix', 'crayon', 'matrixStats')); \
    \
    # Install susieR from source \
    cat('Installing susieR from source...\n'); \
    install.packages('/tmp/susieR_0.12.35.tar.gz', repos=NULL, type='source'); \
    \
    # Cleanup downloaded file \
    unlink('/tmp/susieR_0.12.35.tar.gz'); \
    \
    # Verify installation \
    susie_version <- packageVersion('susieR'); \
    cat('Final susieR version:', as.character(susie_version), '\n'); \
    if (as.character(susie_version) == '0.12.35') { \
        cat('SUCCESS: susieR 0.12.35 installed correctly!\n'); \
    } else { \
        cat('ERROR: Expected 0.12.35 but got', as.character(susie_version), '\n'); \
        quit(status=1); \
    }; \
    "

# Install other R packages WITHOUT updating susieR
RUN Rscript -e " \
    options(repos = c(CRAN = 'https://cloud.r-project.org/')); \
    options(Ncpus = parallel::detectCores()); \
    BiocManager::install(c('dplyr', 'readr', 'data.table', 'Rfast', 'devtools'), ask=FALSE, update=FALSE); \
    BiocManager::install(c('MungeSumstats'), ask=FALSE, update=FALSE); \
    BiocManager::install(c('SNPlocs.Hsapiens.dbSNP155.GRCh37', 'SNPlocs.Hsapiens.dbSNP155.GRCh38'), ask=FALSE, update=FALSE); \
    BiocManager::install(c( \
        'BSgenome.Hsapiens.UCSC.hg19', \
        'BSgenome.Hsapiens.UCSC.hg38', \
        'BSgenome.Hsapiens.1000genomes.hs37d5' \
    ), ask=FALSE, update=FALSE); \
    "

# Install vautils from GitHub
RUN Rscript -e " \
    library(devtools); \
    withr::with_envvar(c('GITHUB_PAT' = ''), { \
        install_github('oyhel/vautils', upgrade='never') \
    }) \
    "

# Final check
RUN Rscript -e " \
    cat('=== FINAL VERIFICATION ===\n'); \
    cat('BiocManager:', as.character(packageVersion('BiocManager')), '\n'); \
    cat('susieR:', as.character(packageVersion('susieR')), '\n'); \
    cat('R version:', R.version.string, '\n'); \
    "

# Install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt rpy2

# Copy application
COPY . .

EXPOSE 5000