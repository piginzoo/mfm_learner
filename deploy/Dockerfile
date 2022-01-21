From brunneis/python:3.8.3-ubuntu-20.04

MAINTAINER piginzoo

RUN cp /etc/apt/sources.list /etc/apt/sources.list.backup
ADD deploy/sources.list /etc/apt/sources.list
RUN apt-get update

RUN mkdir /root/.pip
ADD deploy/pip.conf /root/.pip
ADD requirement.txt /root/requirement.txt
RUN apt install -y git
RUN pip install -r /root/requirement.txt

# time zone
ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN true
RUN apt install -y tzdata
RUN rm /etc/localtime
RUN ln -s /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN echo "Asia/Shanghai">/etc/timezone