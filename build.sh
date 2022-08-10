#!/bin/bash

docker stop lk_sales_program
docker rm lk_sales_program
docker rmi lk_sales_program
docker build . -t lk_sales_program