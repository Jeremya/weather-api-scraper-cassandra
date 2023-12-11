# weather-api-scraper-cassandra
Retrieve weather data and store it in Cassandra

```shell
docker pull cassandra:2.1.22
docker run --name cassandra -p 9042:9042 -d cassandra:2.1.22
docker exec -it cassandra cqlsh
```

```sql
CREATE KEYSPACE weatherdata WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
CREATE TABLE weatherdata.city_weather (
    city text,
    state text,
    country text,
    datetime timestamp,
    temperature float,
    wind_speed float,
    short_forecast text,
    entry_id timeuuid,  -- Adding a timeuuid column
    PRIMARY KEY ((city), datetime, entry_id)
) WITH CLUSTERING ORDER BY (datetime DESC, entry_id DESC);
select * from weatherdata.city_weather ;
```

```shell
pip install -r requirements.txt
python main.py
```

