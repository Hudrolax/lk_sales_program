#!/bin/bash

gunicorn --workers 5 \
--bind 127.0.0.1:8002 \
--threads 4 \
wsgi:server