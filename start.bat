@echo off
docker-compose up -d
start http://localhost:9001
start http://localhost:8080
pause