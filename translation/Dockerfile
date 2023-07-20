FROM nvidia/cuda:12.2.0-base-ubuntu22.04

# set up environment
RUN apt-get update && apt-get install --no-install-recommends --no-install-suggests -y curl
RUN apt-get install unzip
RUN apt-get -y install python3
RUN apt-get -y install python3-pip

COPY requirements.txt /usr/app/requirements.txt
RUN pip3 install -r /usr/app/requirements.txt

# copy code
COPY src/. /usr/app/src/
WORKDIR /usr/app

CMD ["python", "./src/main_translation.py"]