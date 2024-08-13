FROM python:3.10

WORKDIR /fastapi

COPY requirements.txt ./

RUN  apt-get update && apt-get install vim -y
RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install pyjwt

COPY /home/ec2-user/tech-challenge/challenge/API /fastapi

EXPOSE 8080
