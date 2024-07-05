FROM python:3.9-slim

ENV PYTHONUNBUFFERED 1
ENV TZ=Asia/Kathmandu

RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    chromium \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/share/zoneinfo/Asia/Kathmandu /etc/localtime && echo "Asia/Kathmandu" > /etc/timezone

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5000

CMD ["python", "app.py"]
