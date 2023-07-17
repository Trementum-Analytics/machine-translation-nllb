FROM python:3.9

COPY src/. /usr/app/src/
COPY requirements.txt /usr/app/
COPY content/. /usr/app/content/

WORKDIR /usr/app

RUN pip install -r requirements.txt

CMD ["python", "./src/main_detection.py"]