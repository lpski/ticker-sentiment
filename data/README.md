# Starting cluser

```
docker-compose up -d

```



# Teardown
```
docker-compose down -v

or 
docker container ls
docker stop [con-id]
```


# Logs
```
docker ps -a
docker logs elasticsearch
```



# Testing
```
# Check if running
curl -X GET "localhost:9200/_cat/nodes?v&pretty"
curl http://127.0.0.1:9200/_cat/health
```
