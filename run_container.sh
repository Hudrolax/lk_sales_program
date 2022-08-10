#!/bin/bash

docker run \
-d \
--name lk_sales_program \
--net container:www_nginx \
--restart unless-stopped \
-v /app:/home/www/lk_sales_program \
lk_sales_program