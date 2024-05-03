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

RUN wget https://ftp.mcs.anl.gov/pub/darshan/releases/darshan-3.4.4.tar.gz
RUN tar zxvf darshan-3.4.4.tar.gz

WORKDIR /darshan-3.4.4/

RUN bash prepare.sh

WORKDIR /darshan-3.4.4/darshan-util/

RUN ./configure --prefix=/opt/darshan && make && make install

ENV PATH=/opt/darshan/bin:$PATH
ENV LD_LIBRARY_PATH=/opt/darshan/lib:$LD_LIBRARY_PATH

WORKDIR /

RUN git clone https://github.com/hpc-io/drishti-io

WORKDIR /drishti-io

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install .

ENTRYPOINT ["drishti"]
