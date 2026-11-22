# Partie 2 : Kafka avec Docker Compose

Le fichier [`docker-compose.yml`](../docker-compose.yml) démarre deux conteneurs, avec les images Confluent 7.6.1 :

| Conteneur | Rôle | Port |
|---|---|---|
| `bdcc-zookeeper` | Coordination du cluster | `2181` |
| `bdcc-kafka-broker` | Broker Kafka | `9092` |

Le broker expose deux listeners :

- `PLAINTEXT://localhost:9092` pour les clients de la machine hôte, comme l'application Spring.
- `PLAINTEXT_INTERNAL://broker:29092` pour les échanges à l'intérieur du réseau Docker.

## 1. Démarrer les conteneurs

```powershell
docker compose up -d --wait
```

```powershell
docker compose ps
```

```text
NAME                IMAGE                             STATUS                   PORTS
bdcc-kafka-broker   confluentinc/cp-kafka:7.6.1       Up 3 minutes (healthy)   0.0.0.0:9092->9092/tcp
bdcc-zookeeper      confluentinc/cp-zookeeper:7.6.1   Up 3 minutes             0.0.0.0:2181->2181/tcp
```

## 2. Créer un topic

```powershell
docker exec bdcc-kafka-broker kafka-topics --bootstrap-server localhost:9092 --create --topic T1 --partitions 1 --replication-factor 1
```

```text
Created topic T1.
Topic: T1  PartitionCount: 1  ReplicationFactor: 1  Configs:
    Topic: T1  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
```

## 3. Tester avec la console

Consumer, dans un premier terminal :

```powershell
docker exec -it bdcc-kafka-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic T1
```

Producer, dans un second terminal :

```powershell
docker exec -it bdcc-kafka-broker kafka-console-producer --bootstrap-server localhost:9092 --topic T1
```

```text
> Bonjour depuis Docker
> Message 2 via le conteneur
> Kafka Broker OK
```

Résultat côté consumer :

```text
Bonjour depuis Docker
Message 2 via le conteneur
Kafka Broker OK
Processed a total of 3 messages
```

Relecture de l'historique :

```powershell
docker exec -it bdcc-kafka-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic T1 --from-beginning
```

## 4. Arrêter les conteneurs

```powershell
docker compose down
```
