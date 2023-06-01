# tdspark
Exemple de régression linéaire avec pyspark
# Lancer docker
# Se placer dans le répertoire où se trouve le docker file
# builder
docker build -t spark_docker_v1 .
# lancer 
docker run --rm -it -p 4040:4040 spark_docker_v1
# retrouver le container 
docker ps
# exécuter 
docker exec -ti 5ad97e3de038 /bin/bash
# lancer la commande python
python query.py
