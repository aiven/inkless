# Running

```shell
docker compose -f producers-docker-compose.yml up
```

If you need to run a different number of producers:
```shell
docker compose -f producers-docker-compose.yml up --scale producer=2
```

See Grafana at http://127.0.0.1:3000/d/ee4jhhuz3t2bkb/kafka-inkless (username `admin`, password `admin`).
