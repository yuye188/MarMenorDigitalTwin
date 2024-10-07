FROM python:3.11.2

RUN apt-get update \
  && apt-get install -y vim \ 
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless \
	 gunicorn \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

RUN mkdir /app

COPY ./* /app
ADD ./templates/ /app/templates/
ADD ./static/ /app/static/

WORKDIR /app

RUN pip3 install -r requirements.txt

EXPOSE 3838

#CMD ["python3", "app.py"]
