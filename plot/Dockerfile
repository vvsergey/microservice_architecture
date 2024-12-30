FROM python:latest
WORKDIR /usr/src/app
COPY ./src .
COPY ./logs ./logs
COPY ./requirements.txt ./
VOLUME /usr/src/app/metric/logs:/plot/logs
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python", "./plot.py" ]
