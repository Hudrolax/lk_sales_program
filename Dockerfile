FROM python

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /app

# copy project
COPY . .

# install dependencies
RUN python3 -m pip install --upgrade pip
RUN pip3 install -r requirements.txt

RUN chmod +x /app/entrypoint.sh

# run entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]