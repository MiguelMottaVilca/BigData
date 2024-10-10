import random
import time

users = [f"user{str(i).zfill(3)}" for i in range(1, 21)]
queries = ["search", "click", "view", "login", "logout", "purchase", "comment"]

# Generar registros de log
with open('files/user_logs.txt', 'w') as file:
    for _ in range(10000):
        user = random.choice(users)
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(random.randint(1609459200, 1612137600)))
        query = random.choice(queries)
        file.write(f"{user}\t{timestamp}\t{query}\n")

print("Archivo 'files/user_logs.txt' generado.")