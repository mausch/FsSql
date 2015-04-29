docker --version || wget -qO- https://get.docker.com/ | sh
sleep 1
docker build -t fssql . && docker run --rm fssql
docker rmi fssql || true
