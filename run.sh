#!/bin/bash

echo " Build des containers..."
docker-compose build

echo "Services Up..."
docker-compose up
