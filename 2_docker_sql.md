## Question 3

```sql
SELECT
	CAST(lpep_dropoff_datetime AS DATE) AS "day",
	COUNT(1)
FROM
	ny_yellow_taxi t
WHERE
	lpep_pickup_datetime >= '2019-01-15' AND lpep_pickup_datetime < '2019-01-16' AND
	lpep_dropoff_datetime >= '2019-01-15' AND lpep_dropoff_datetime < '2019-01-16'
GROUP BY
	"day"
```

## Question 4

```sql
SELECT
	CAST(lpep_pickup_datetime AS DATE) AS "pickup",
	MAX(trip_distance) AS "max_trip_distance"
FROM
	ny_yellow_taxi t
GROUP BY
	pickup
ORDER BY
	max_trip_distance DESC
```

## Question 5

```sql
SELECT
	CAST(lpep_pickup_datetime AS DATE) AS "pickup",
	passenger_count,
	count(1)
FROM
	ny_yellow_taxi t
WHERE
	lpep_pickup_datetime >= '2019-01-01' AND lpep_pickup_datetime < '2019-01-02'
	AND (passenger_count = 3 OR passenger_count = 2)
GROUP BY
	pickup, passenger_count
```

## Question 6

```sql
SELECT
	MAX(tip_amount) as max_tip,
	tzd."Zone"
FROM
	ny_yellow_taxi t
	JOIN taxi_zone tz ON t."PULocationID" = tz."LocationID"
	JOIN taxi_zone tzd ON t."DOLocationID" = tzd."LocationID"
WHERE
	tz."Zone" = 'Astoria'
GROUP BY
	tzd."Zone"
ORDER BY
	max_tip DESC
```
