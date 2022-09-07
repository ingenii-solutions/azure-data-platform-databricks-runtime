FROM databricksruntime/standard:9.x

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
ENV PYTHONUNBUFFERED 1

RUN apt-get update && \
        apt-get install -yq libsasl2-dev build-essential g++ unixodbc-dev && \
        apt-get clean
RUN apt-get install -y libpython3.8-dev

RUN ln -sv /usr/include/python3.8/* /usr/include/