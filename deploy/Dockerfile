FROM ubuntu:20.04
MAINTAINER piginzoo

RUN cp /etc/apt/sources.list /etc/apt/sources.list.backup
ADD deploy/sources.list /etc/apt/sources.list
RUN apt-get update

RUN apt-get install -y python3.8
RUN apt-get install -y python3-pip

RUN mkdir /root/.pip
ADD deploy/pip.conf /root/.pip
ADD requirement.txt /root/requirement.txt
RUN apt install -y git

RUN pip install -r /root/requirement.txt

RUN mkdir /root/depends

RUN git clone --depth 1 https://gitee.com/piginzoo/alphalens.git /root/depends/alphalens
WORKDIR /root/depends/alphalens
RUN python setup.py install

RUN git clone --depth 1 https://gitee.com/piginzoo/jaqs-fxdayu.git /root/depends/jaqs-fxdayu
WORKDIR /root/depends/jaqs-fxdayu
RUN python setup.py install

# time zone
ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN true
RUN apt install -y tzdata
RUN rm /etc/localtime
RUN ln -s /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN echo "Asia/Shanghai">/etc/timezone