# vintage
Apache Spark `DStreams` playground

# Run (in order on separate shell) 
* `nc -lk 9999`
* `sbt run`

# Output
Accumulators report following:
```text
RDD size: 0
------------------------------------------------------------
LongAccumulator(id: 0, name: Some(test-timer), value: 1284633)
LongAccumulator(id: 1, name: Some(fm-timer), value: 0)
LongAccumulator(id: 2, name: Some(m-timer), value: 0)
LongAccumulator(id: 3, name: Some(rbk-timer), value: 0)
------------------------------------------------------------
RDD size: 33
------------------------------------------------------------
LongAccumulator(id: 0, name: Some(test-timer), value: 1278166)
LongAccumulator(id: 1, name: Some(fm-timer), value: 53805)
LongAccumulator(id: 2, name: Some(m-timer), value: 292601)
LongAccumulator(id: 3, name: Some(rbk-timer), value: 153335)
------------------------------------------------------------
RDD size: 0
------------------------------------------------------------
LongAccumulator(id: 0, name: Some(test-timer), value: 1081836)
LongAccumulator(id: 1, name: Some(fm-timer), value: 0)
LongAccumulator(id: 2, name: Some(m-timer), value: 0)
LongAccumulator(id: 3, name: Some(rbk-timer), value: 0)
------------------------------------------------------------
```

Tasks' `Duration` is 1, 10, 1 ms respectively.
