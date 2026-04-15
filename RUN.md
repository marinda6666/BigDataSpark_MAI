# Запуск лабы

## 1) Запуск всех контейнеров

```bash
docker compose up -d
```

PostgreSQL при старте создаст таблицу `mock_data` и загрузит все 10 CSV из `исходные данные/`. 

**Подключение PostgreSQL:**
- Host: localhost
- Port: 5433
- Database: bigdata
- Username: bigdata
- Password: bigdata

## 2) Построить модель «звезда» в PostgreSQL

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark-apps/jars/postgresql-42.7.1.jar \
  /opt/spark-apps/jobs/etl_to_star.py
```

**Проверить таблицы:**
```bash
docker compose exec postgres psql -U bigdata -d bigdata -c "SELECT table_name FROM information_schema.tables WHERE table_schema = 'star'"
```

## 3) Построить витрины в ClickHouse

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark-apps/jars/postgresql-42.7.1.jar,/opt/spark-apps/jars/clickhouse-jdbc-0.4.6.jar \
  /opt/spark-apps/jobs/reports_to_clickhouse.py
```

**Проверить таблицы:**
```bash
docker compose exec clickhouse clickhouse-client --query "SHOW TABLES"
```

**Примеры запросов (docker compose exec clickhouse clickhouse-client --query "ЗАПРОС"):**
```sql
SELECT * FROM report_product_sales ORDER BY product_rank LIMIT 10;
SELECT * FROM report_customer_sales ORDER BY customer_rank LIMIT 10;
SELECT * FROM report_time_sales ORDER BY year, month;
SELECT * FROM report_store_sales ORDER BY store_rank LIMIT 5;
SELECT * FROM report_supplier_sales ORDER BY supplier_rank LIMIT 5;
SELECT * FROM report_quality ORDER BY rating_rank_desc LIMIT 10;
```

## 4) Построить витрины в MongoDB

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark-apps/jars/postgresql-42.7.1.jar,/opt/spark-apps/jars/mongo-spark-connector_2.12-10.4.0.jar,/opt/spark-apps/jars/mongodb-driver-sync-4.11.1.jar,/opt/spark-apps/jars/mongodb-driver-core-4.11.1.jar,/opt/spark-apps/jars/bson-4.11.1.jar \
  /opt/spark-apps/jobs/reports_to_mongodb.py
```

**Проверить коллекции:**
```bash
docker compose exec mongodb mongosh --eval "db = db.getSiblingDB('reports'); db.getCollectionNames()"
```

**Проверка данных (пример):**
```bash
docker compose exec mongodb mongosh --eval "db = db.getSiblingDB('reports'); db.report_product_sales.find().sort({product_rank: 1}).limit(5)"
```
