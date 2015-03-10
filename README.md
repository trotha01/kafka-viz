Kafka-Viz
===

A way to quickly and easily run, test, and visualize kafka

![kafka image](https://github.com/trotha01/kafka-viz/blob/master/assets/kafka-viz.png)

Prerequisites
===
 - [kafka](http://kafka.apache.org/downloads.html)
 - [go](https://golang.org/doc/install)

QuickStart
===
Start up zookeeper and kafka

```
$ go install github.com/trotha01/kafka-viz
$ kafka-viz
```

Open browser to localhost:8090

Configuration
===
Kafka-Viz can get it's configuration from the environment

| Env Variable | Default       | Use                                                                         |
| ------------ | ------------- | ----------                                                                  |
| HOST         | 127.0.0.1     | The host for http.ListenAndServe                                            |
| PORT         | 8090          | The host for http.ListenAndServe                                            |
| LOG_DIR      | .             | The logfile directory, relative to kafka-viz                                |
| LOG_FILE     | STDOUT        | The logfile. STDOUT can be used instead of a file (LOG_DIR will be ignored) |
| KAFKA_HOST   | localhost     | The host of the kafka broker                                                |
| KAFKA_PORT   | 9092          | The port of the kafka broker                                                |
| PERMISSIONS  | R             | R, W, or RW, for read/write permisisons to kafka                            |



What is Kafka?
===
[Kafka](http://kafka.apache.org/) is designed to allow a single cluster to serve as the central data backbone for a large organization. It can be elastically and transparently expanded without downtime. Data streams are partitioned and spread over a cluster of machines to allow data streams larger than the capability of any single machine and to allow clusters of co-ordinated consumers

