from kafka import KafkaProducer
import json
import time
import csv

# Connexion à Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Colonnes du dataset NSL-KDD
COLUMNS = [
    'duration', 'protocol_type', 'service', 'flag',
    'src_bytes', 'dst_bytes', 'land', 'wrong_fragment',
    'urgent', 'hot', 'num_failed_logins', 'logged_in',
    'num_compromised', 'root_shell', 'su_attempted',
    'num_root', 'num_file_creations', 'num_shells',
    'num_access_files', 'num_outbound_cmds', 'is_host_login',
    'is_guest_login', 'count', 'srv_count', 'serror_rate',
    'srv_serror_rate', 'rerror_rate', 'srv_rerror_rate',
    'same_srv_rate', 'diff_srv_rate', 'srv_diff_host_rate',
    'dst_host_count', 'dst_host_srv_count',
    'dst_host_same_srv_rate', 'dst_host_diff_srv_rate',
    'dst_host_same_src_port_rate', 'dst_host_srv_diff_host_rate',
    'dst_host_serror_rate', 'dst_host_srv_serror_rate',
    'dst_host_rerror_rate', 'dst_host_srv_rerror_rate',
    'label', 'difficulty'
]

print("Démarrage du producteur... Envoi des événements vers Kafka")

# Lire le fichier et envoyer ligne par ligne
with open('/data/KDDTrain+.txt', 'r') as f:
    reader = csv.reader(f)
    for i, row in enumerate(reader):
        if len(row) != len(COLUMNS):
            continue

        # Créer l'événement
        event = dict(zip(COLUMNS, row))
        event['timestamp'] = time.time()
        event['event_id'] = i

        # Envoyer dans Kafka
        producer.send('security-events', event)

        # Afficher progression toutes les 100 lignes
        if i % 100 == 0:
            print(f"Envoyé : {i} événements")

        # Pause réaliste : 1 événement toutes les 50ms
        time.sleep(0.05)

print("Fin du fichier !")
