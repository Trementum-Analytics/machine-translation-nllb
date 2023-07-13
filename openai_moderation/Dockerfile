FROM python:3.9

COPY src/. /usr/app/src/
COPY requirements.txt /usr/app/

WORKDIR /usr/app

RUN pip install -r requirements.txt

CMD ["python", "./src/main_moderation.py"]