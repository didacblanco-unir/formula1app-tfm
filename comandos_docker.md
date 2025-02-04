# Docker

Para iniciar Postgre, en el directorio donde esté el docker-compose:
```bash
docker-compose up -d
```
Para iniciar el contenedor de PySpark:
```bash
docker exec -it pyspark bash 
```

Para encontrar el enlace y el token para entrar a Jupyter:
```bash
docker logs --tail 10 pyspark
```

Antes de levantar ambos contenedores, debemos crear la red de Docker llamada `custom_network`:
```bash
docker network create -d bridge custom_network
```