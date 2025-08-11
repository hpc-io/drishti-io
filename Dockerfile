FROM ubuntu:22.04

RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y \
    python3.10 \
    pip \
    make \
    git \
    wget \
    libssl-dev \
    libcurl4-openssl-dev \
    libtool \
    autoconf \
    zlib1g \
    libtool
RUN rm -rf /var/lib/apt/lists/*

RUN wget https://web.cels.anl.gov/projects/darshan/releases/darshan-3.4.6.tar.gz
RUN tar zxvf darshan-3.4.6.tar.gz
WORKDIR /darshan-3.4.6/
RUN ./prepare.sh

WORKDIR /darshan-3.4.6/darshan-util/
RUN ./configure --enable-pydarshan --enable-shared
RUN make
RUN make install

WORKDIR /

RUN git clone https://github.com/hpc-io/drishti-io

WORKDIR /drishti-io

RUN pip install -r requirements.txt
RUN pip install .

RUN echo "/usr/local/lib/" > /etc/ld.so.conf.d/libdarshan.conf
RUN ldconfig

ENTRYPOINT ["drishti"]
