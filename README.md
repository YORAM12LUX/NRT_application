# PRINCIPAL COMPONENTS 




1. Topic

    Un canal logique où les messages sont publiés.

    Les producers écrivent dans un topic.

    Les consumers lisent depuis un topic.

    Chaque topic est divisé en partitions.

2. Partition

    Une division physique d’un topic.

    Permet de répartir les données sur plusieurs brokers pour scalabilité.

    Chaque message a un offset (position unique dans la partition).

    Une seule copie leader est utilisée pour lecture/écriture, les autres sont des réplicas.

3. Message (Record)

    Un élément de données envoyé à Kafka.

    Composé d’une clé (optionnelle), d’une valeur, et d’un timestamp.

    Peut contenir des métadonnées (headers).

4. Producer

    Application qui publie des messages vers un ou plusieurs topics.

    Peut choisir la partition via la clé.

    Gère les acknowledgements pour garantir la livraison.

5. Consumer

    Application qui lit les messages depuis un topic.

    Appartient à un consumer group.

    Kafka garantit qu’une partition est lue par un seul consumer du même groupe.

6. Consumer Group

    Groupe de consumers qui se partagent la lecture des partitions.

    Permet le parallélisme.

    Chaque consumer lit un sous-ensemble de partitions.

7. Broker

    Un serveur Kafka qui stocke les messages et sert les requêtes.

    Un cluster Kafka est constitué de plusieurs brokers.

    Un broker peut être leader ou follower pour une partition.

8. Cluster

    Ensemble de brokers Kafka.

    Offre tolérance aux pannes et répartition de charge.

    Gère la réplication des partitions.

9. Replication

    Chaque partition a :

    Un leader (responsable des écritures et lectures).

    Des followers (répliques synchronisées pour la haute disponibilité).

10. Zookeeper / KRaft

    Zookeeper : Utilisé pour gérer la configuration et l’élection des leaders (versions < 2.8).

    KRaft mode : Nouveau mode sans Zookeeper, intégré directement dans Kafka.

11. Kafka Connect

    Framework pour intégrer Kafka avec des systèmes externes (bases, stockage, moteurs de recherche…).

    Source connector : import de données dans Kafka.

    Sink connector : export de données depuis Kafka.

12. Kafka Streams

    Bibliothèque Java pour créer des applications de traitement en temps réel directement sur les flux Kafka.