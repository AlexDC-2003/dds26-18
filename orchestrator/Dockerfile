FROM python:3.12-slim

WORKDIR /home/app

COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["hypercorn", "-b", "0.0.0.0:8000", "--workers", "4", "app:app"]
