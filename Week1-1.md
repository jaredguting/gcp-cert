# Engineer Data in Google Cloud: Challenge Lab Week 1


###  Cleaning the training data
```sql
CREATE OR REPLACE TABLE
  taxirides.TABLE_NAME AS
SELECT
  (tolls_amount + fare_amount) AS FARE_AMOUNT,
  pickup_datetime,
  pickup_longitude AS pickuplon,
  pickup_latitude AS pickuplat,
  dropoff_longitude AS dropofflon,
  dropoff_latitude AS dropofflat,
  passenger_count AS passengers,
FROM
  taxirides.historical_taxi_rides_raw
WHERE
  RAND() < 0.001
  AND trip_distance > 2 -- TRIP_DISTANCE
  AND fare_amount >= fare_amount_757 -- FARE_AMOUNT_NUMBER
  AND pickup_longitude > -78
  AND pickup_longitude < -70
  AND dropoff_longitude > -78
  AND dropoff_longitude < -70
  AND pickup_latitude > 37
  AND pickup_latitude < 45
  AND dropoff_latitude > 37
  AND dropoff_latitude < 45
  AND passenger_count > 2 -- PASSENGER_COUNT
```

### Creating a BigQuery ML model

```sql
CREATE OR REPLACE MODEL taxirides.fare_model_479
TRANSFORM(
  * EXCEPT(pickup_datetime) -- model name

  , ST_Distance(ST_GeogPoint(pickuplon, pickuplat), ST_GeogPoint(dropofflon, dropofflat)) AS euclidean
  , CAST(EXTRACT(DAYOFWEEK FROM pickup_datetime) AS STRING) AS dayofweek
  , CAST(EXTRACT(HOUR FROM pickup_datetime) AS STRING) AS hourofday
)
OPTIONS(input_label_cols=['FARE_AMOUNT'], model_type='linear_reg')
AS

SELECT * FROM taxirides.taxi_training_data_458 --TABLE_NAME
```

### Performing batch prediction on new data
```sql
CREATE OR REPLACE TABLE taxirides.2015_fare_amount_predictions
  AS
SELECT * FROM ML.PREDICT(MODEL taxirides.fare_model_479,(
  SELECT * FROM taxirides.report_prediction_data)
) -- model name
```
