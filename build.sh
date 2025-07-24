aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 794038229063.dkr.ecr.us-east-2.amazonaws.com
docker build -t firstrepository .
docker tag firstrepository:latest 794038229063.dkr.ecr.us-east-2.amazonaws.com/firstrepository:latest
docker push 794038229063.dkr.ecr.us-east-2.amazonaws.com/firstrepository:latest