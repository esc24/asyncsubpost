FROM ubuntu:14.04
MAINTAINER Ed Campbell <email@email.com>
RUN apt-get update && apt-get install -y python-pip python-dev
COPY ./requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
ENTRYPOINT ["python3", "asyncsubpush.py"]
