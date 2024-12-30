FROM python:latest 
WORKDIR /usr/src/app
COPY ./src .
COPY ./logs ./logs
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python", "./metric.py" ]