FROM python:3.9

WORKDIR /usr/app

# Install git
RUN apt-get update && apt-get install -y git

# Clone the repository from GitHub
RUN git clone https://github.com/Trementum-Analytics/machine-translation-nllb.git .

RUN pip install -r requirements.txt

CMD ["python", "./src/main_moderation.py"]