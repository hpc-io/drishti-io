FROM ubuntu

RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y \
    python3 \
    pip \
    make \
    git \
    wget \
    libssl-dev \
    libcurl4-openssl-dev \
    libtool \
    autoconf \
    automake
RUN rm -rf /var/lib/apt/lists/*

RUN wget ftp://ftp.mcs.anl.gov/pub/darshan/releases/darshan-3.4.0.tar.gz
RUN tar zxvf darshan-3.4.0.tar.gz

WORKDIR /darshan-3.4.0/

RUN bash prepare.sh

WORKDIR /darshan-3.4.0/darshan-util/

RUN ./configure && make && make install

WORKDIR /

RUN git clone https://github.com/hpc-io/drishti

WORKDIR /drishti

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install .

ENTRYPOINT ["drishti"]