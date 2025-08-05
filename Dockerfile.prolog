# Use Debian slim as the base image to install SWI-Prolog 9.3.11
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libtcmalloc-minimal4 \
    libarchive13 \
    libyaml-0-2 \
    libgmp10 \
    libossp-uuid16 \
    libssl3 \
    ca-certificates \
    libdb5.3 \
    libpcre2-8-0 \
    libedit2 \
    libgeos3.11.1 \
    libspatialindex6 \
    unixodbc \
    odbc-postgresql \
    tdsodbc \
    libmariadbclient-dev-compat \
    libsqlite3-0 \
    libserd-0-0 \
    python3 \
    libpython3.11 \
    libraptor2-0 && \
    rm -rf /var/lib/apt/lists/*

# Set environment variable
ENV LANG C.UTF-8

# Install build dependencies and SWI-Prolog 9.3.11
RUN set -eux; \
    SWIPL_VER=9.3.11; \
    SWIPL_CHECKSUM=b8bffac671ee031ee34d033c168fed0a6f4ea0a906e2a13f5a19f00b59cd4b55; \
    BUILD_DEPS='make cmake ninja-build gcc g++ wget git pkg-config m4 libtool automake autoconf \
                libarchive-dev libgmp-dev libossp-uuid-dev libpcre2-dev libreadline-dev libedit-dev \
                libssl-dev zlib1g-dev libdb-dev unixodbc-dev libsqlite3-dev libserd-dev libraptor2-dev \
                libyaml-dev libgoogle-perftools-dev libpython3-dev'; \
    apt-get update; \
    apt-get install -y --no-install-recommends $BUILD_DEPS; \
    rm -rf /var/lib/apt/lists/*; \
    mkdir /tmp/src; \
    cd /tmp/src; \
    wget -q https://www.swi-prolog.org/download/devel/src/swipl-$SWIPL_VER.tar.gz; \
    echo "$SWIPL_CHECKSUM  swipl-$SWIPL_VER.tar.gz" >> swipl-$SWIPL_VER.tar.gz-CHECKSUM; \
    sha256sum -c swipl-$SWIPL_VER.tar.gz-CHECKSUM; \
    tar -xzf swipl-$SWIPL_VER.tar.gz; \
    mkdir swipl-$SWIPL_VER/build; \
    cd swipl-$SWIPL_VER/build; \
    cmake -DCMAKE_BUILD_TYPE=PGO \
          -DSWIPL_PACKAGES_X=OFF \
          -DSWIPL_PACKAGES_JAVA=OFF \
          -DCMAKE_INSTALL_PREFIX=/usr \
          -G Ninja \
          ..; \
    ninja; \
    ninja install; \
    rm -rf /tmp/src; \
    apt-get purge -y --auto-remove $BUILD_DEPS

# Set the working directory for Prolog files
WORKDIR /app/pl

# Copy Prolog files from the local directory
COPY ./pl /app/pl

# Install Prolog libraries using pack_install
RUN apt-get update && apt-get install -y git graphviz && \
    swipl -g "pack_install('prolog_library_collection', [interactive(false)])" -t halt && \
    swipl -g "pack_install('prolog_graphviz', [interactive(false)])" -t halt && \
    swipl -g "pack_install('interpolate', [interactive(false)])" -t halt 
    

# Expose port 4242 for the Prolog server
EXPOSE 4242

# Command to load Prolog files and start the server on port 4242
ENTRYPOINT ["swipl", "-s", "load_kbs.pl", "-s", "meta_interpreter.pl", "-s", "queries.pl", "-s", "rules.pl", "-g", "server_start(4242)", "-g", "thread_get_message(_)"]
