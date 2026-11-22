# Partie 1 : Kafka en local avec ZooKeeper

Version utilisée : **Apache Kafka 3.9.2 (Scala 2.13)**, sous Windows 11 avec Java 21.
La branche 3.9 est la dernière à prendre en charge ZooKeeper, car Kafka 4 fonctionne uniquement en mode KRaft.

## 1. Télécharger Kafka

Télécharger `kafka_2.13-3.9.2.tgz` depuis <https://kafka.apache.org/downloads>, puis l'extraire dans un dossier au **chemin court**, par exemple `C:\kafka`.

```powershell
tar -xzf kafka_2.13-3.9.2.tgz
```

```powershell
Rename-Item kafka_2.13-3.9.2 C:\kafka
```

## 2. Démarrer ZooKeeper

```powershell
C:\kafka\bin\windows\zookeeper-server-start.bat C:\kafka\config\zookeeper.properties
```

ZooKeeper écoute sur le port `2181`. Il doit être démarré avant le broker, car il coordonne le cluster.

## 3. Démarrer le serveur Kafka

Dans un deuxième terminal :

```powershell
$env:KAFKA_HEAP_OPTS = "-Xmx1G -Xms1G"
```

```powershell
C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\server.properties
```

Le broker écoute sur le port `9092`. Le journal confirme le démarrage :

```text
[2026-11-23 00:07:03,218] INFO [KafkaServer id=0] started (kafka.server.KafkaServer)
```

## 4. Tester avec la console

Créer le topic `T1` :

```powershell
C:\kafka\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic T1 --partitions 1 --replication-factor 1
```

```text
Created topic T1.
Topic: T1  PartitionCount: 1  ReplicationFactor: 1  Configs:
    Topic: T1  Partition: 0  Leader: 0  Replicas: 0  Isr: 0
```

Lancer le consumer dans un terminal :

```powershell
C:\kafka\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic T1
```

Lancer le producer dans un autre terminal, puis saisir les messages :

```powershell
C:\kafka\bin\windows\kafka-console-producer.bat --bootstrap-server localhost:9092 --topic T1
```

```text
> Bonjour Kafka
> Message 2 depuis la console
> Event Driven Architecture - ENSET
```

Résultat côté consumer :

```text
Bonjour Kafka
Message 2 depuis la console
Event Driven Architecture - ENSET
Processed a total of 3 messages
```

Kafka conserve les messages après leur lecture. Un nouveau consumer peut donc relire tout l'historique :

```powershell
C:\kafka\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic T1 --from-beginning
```

```text
Bonjour Kafka
Message 2 depuis la console
Event Driven Architecture - ENSET
Processed a total of 3 messages
```

## Problèmes rencontrés sous Windows

| Problème | Cause | Solution |
|---|---|---|
| Le broker ne démarre pas et affiche `'wmic' is not recognized` | `kafka-server-start.bat` appelle `wmic`, retiré des versions récentes de Windows 11 | Définir `KAFKA_HEAP_OPTS` avant le lancement pour éviter cet appel |
| `The input line is too long` | Le classpath dépasse la limite de `cmd.exe` si le dossier d'installation a un chemin long | Installer Kafka dans un chemin court comme `C:\kafka` |
