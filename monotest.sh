docker --version || wget -qO- https://get.docker.com/ | sh
docker build -t fssql . && docker run --rm fssql
docker rmi fssql || true
