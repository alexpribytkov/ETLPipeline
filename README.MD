# **ETL Pipeline** 

This project is an example of a full-fledged ETL process. Open APIs and CSV tables are used as data sources. 
In this project uses the following stack of technology:
- The main tool for ETL is **Apache Spark**. 
- For code debugging, there is a **Jupyter Notebook** with **Pandas** and **Spark** installed inside.
- **Apache Airflow** is used as the orchestrator. 
- All data marts are stored in **ClickHouse**. 
- Visualization is deployed on **Apache Superset**.
- A shared folder called data_lake is used as a data warehouse (it can be regarded as S3).

![Project](ETL_pipeline.jpg)

## Building the Docker Images

To build the Docker images, navigate to the root directory of the project in your terminal and run the following command in background:

```shell
docker-compose up -d
```

To show all the running docker containers

```shell
docker ps
```

To stop all the docker containers

```shell
docker-compose down
```

To go inside the docker container

```shell
docker exec -it <containerID> bash
```

## Prerequisites
- Docker: Make sure you have Docker installed on your system. You can download it from [here](https://www.docker.com/products/docker-desktop).
- Docker Compose: Also ensure that Docker Compose is installed. You can find it [here](https://docs.docker.com/compose/install/).

## Services

| Service         | Port                                            | User    | Password |
| --------------- | ----------------------------------------------- | ------- | -------- |
| Airflow         | [http://localhost:8080](http://localhost:8080/) | airflow | airflow  |
| Airflow(meta DB)| [http://localhost:5430](http://localhost:5430/) | airflow | airflow  |
| ClickHouse      |                                                 |         |          |
| Jupyter Lab     |                                                 |         |          |
| Apache Superset |                                                 |         |          |

### Source

Sources of synthetic data are located in the `synthetic_data` folder. The remaining data can be obtained via `Open API`.

### PostgreSQL

The PostgreSQL service is used as the backend database for Airflow. The service uses the `postgres:15` image and exposes the default PostgreSQL port `5432`. The data for the service is persisted in the `postgres-db-volume` volume.

### Airflow

The Airflow service is split into four separate services for the webserver, scheduler, executor, worker, metadata database. Each service is built from the `Dockerfile` in the `./airflow-dev` directory. The services share common environment variables and volumes defined in the `x-airflow-common` section.