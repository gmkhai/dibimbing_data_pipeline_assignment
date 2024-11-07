# Dibimbing, Data Engineering Bootcamp

1. Clone This Repo.
2. Run `make docker-build` for x86 user, or `make docker-build-arm` for arm chip user.

---
```
## docker-build                 - Build Docker Images (amd64) including its inter-container network.
## docker-build-arm             - Build Docker Images (arm64) including its inter-container network.
## postgres                     - Run a Postgres container
## spark                        - Run a Spark cluster, rebuild the postgres container, then create the destination tables
## jupyter                      - Spinup jupyter notebook for testing and validation purposes.
## airflow                      - Spinup airflow scheduler and webserver.
## kafka                        - Spinup kafka cluster (Kafka+Zookeeper).
## datahub                      - Spinup datahub instances.
## metabase                     - Spinup metabase instance.
## clean                        - Cleanup all running containers related to the challenge.
```

---

# Documentation Assignment

This Assignment have files at some folders.
- `screenshot` is folder results and log when I am run producer kafka and consumer kafka using spark. I have difference when kafka stream using output mode `update` and `complete`. when i am using `update` mode result at console updated but not show all data, difference when i am using `complete` mode all data show in console from first `event_time` until current `event_time` and updated data show at console.

- `scripts\event_assignment_producer.py` this file code i am rewrite for understand of code and add some column, but i am not change order and this use case, because I want to check how late data send to message topic using producer kafka.

- `spark-scripts\spark-assignment-event-consumer.py` this file code i am rewrite too but add some code using watermark for handling aggregation late data, i am add handling convert data datetime unix type to datetime for compatible using matermark. At this file i am using complete mode for show console log all data


If you want to run this assignment you can run:
- `make spark-produce-assignment` for runing producer for create message topic
- `make spark-consume-assignment` for runing consumer for show result data by aggragetion using complete output mode with group  by window and watermark