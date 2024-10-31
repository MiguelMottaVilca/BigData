## HIVE

1. Wordcount usando Hive

``` 
-- Cargar los datos en una tabla temporal
CREATE TABLE temp_logs (line STRING);

-- Cargar datos desde el archivo
LOAD DATA INPATH '/ruta/del/archivo' INTO TABLE temp_logs;
---/user/hadoop/files/wordcount_log.txt

-- Contar la frecuencia de cada palabra
SELECT word, COUNT(1) as count
FROM (
    SELECT explode(split(line, ' ')) as word
    FROM temp_logs
) temp
GROUP BY word
ORDER BY count DESC;
``` 

2. Calculando el número de entradas en el log por cada usuario
``` 

-- Crear una tabla para almacenar logs de usuarios
CREATE TABLE user_logs (username STRING, log_time STRING, query_text STRING);

-- Cargar datos en la tabla desde el archivo log
LOAD DATA INPATH '/user/hadoop/files/user_logs.txt' INTO TABLE user_logs;

-- Contar el número de entradas en el log por usuario
SELECT username, COUNT(*) as log_count
FROM user_logs
GROUP BY username
ORDER BY log_count DESC;
``` 

3. Calculando el promedio de visitas por cada usuario

```
-- Calcular el promedio de visitas por usuario
SELECT username, AVG(visits) as average_visits
FROM (
    SELECT username, COUNT(*) as visits
    FROM user_logs
    GROUP BY username
) user_visits
GROUP BY username
ORDER BY average_visits DESC;
``` 

4. Identificar cuáles usuarios visitan “Página mejores rankeadas” en promedio
``` 
-- Crear una tabla para almacenar información de páginas y su rankeo
CREATE TABLE page_rank (username STRING, page STRING, rank FLOAT);

-- Cargar datos en la tabla desde el archivo de ranking
LOAD DATA INPATH '/user/hadoop/files/page_rank.txt' INTO TABLE page_rank;

-- Identificar usuarios que visitan páginas con un rank mayor a 0.5
SELECT username, AVG(rank) as avg_rank
FROM page_rank
WHERE rank > 0.5
GROUP BY username
ORDER BY avg_rank DESC;
``` 

## Generador de archivos
1. Generar archivo para Wordcount

``` 
import random

# Generar líneas de texto aleatorias
words = ["hadoop", "hive", "mapreduce", "data", "analytics", "python", "java", "sql", "bash"]
lines = [" ".join(random.choices(words, k=random.randint(5, 15))) for _ in range(100)]

# Guardar en un archivo
with open('wordcount_log.txt', 'w') as file:
    file.write("\n".join(lines))

print("Archivo 'wordcount_log.txt' generado.")
``` 

2. Generar archivo para calcular el número de entradas en el log por cada usuario
``` 
import random
import time

users = [f"user{str(i).zfill(3)}" for i in range(1, 21)]
queries = ["search", "click", "view", "login", "logout", "purchase", "comment"]

# Generar registros de log
with open('user_logs.txt', 'w') as file:
    for _ in range(200):
        user = random.choice(users)
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(random.randint(1609459200, 1612137600)))
        query = random.choice(queries)
        file.write(f"{user}\t{timestamp}\t{query}\n")

print("Archivo 'user_logs.txt' generado.")
``` 

3. Generar archivo para calcular el promedio de visitas por cada usuario
Este archivo puede ser el mismo que el de user_logs.txt, ya que contiene los logs de visitas de los usuarios. Si ya generaste el archivo user_logs.txt, puedes reutilizarlo para este ejercicio.


4. Generar archivo para identificar cuáles usuarios visitan “Página mejores rankeadas”

``` 
import random

pages = [f"page{str(i).zfill(2)}" for i in range(1, 16)]

# Generar registros de rankeo de páginas
with open('page_rank.txt', 'w') as file:
    for _ in range(200):
        user = random.choice(users)
        page = random.choice(pages)
        rank = round(random.uniform(0, 1), 2)
        file.write(f"{user}\t{page}\t{rank}\n")

print("Archivo 'page_rank.txt' generado.")
``` 

### Guardar los archivos
``` 
hdfs dfs -put wordcount_log.txt /ruta/en/hdfs/
hdfs dfs -put user_logs.txt /ruta/en/hdfs/
hdfs dfs -put page_rank.txt /ruta/en/hdfs/
``` 

``` 

hdfs dfs -mkdir -p /user/hadoop/files/
chmod 755 /home/ec2-user/files/wordcount_log.txt
hadoop fs -put /home/ec2-user/files/user_logs.txt /user/hadoop/files/
hadoop fs -ls /user/hadoop/files/

sudo chown hadoop:hadoop archivo
``` 