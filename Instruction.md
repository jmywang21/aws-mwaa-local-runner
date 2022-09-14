# What this repo contains

    dags/
      requirements.txt
      tutorial.py
      assignment_etl.py
      data-eng-interview-cc9d14354dd3.json
    docker/
      .gitignore
      mwaa-local-env
      README.md
      config/
        airflow.cfg
        constraints.txt
        requirements.txt
        webserver_config.py
      script/
        bootstrap.sh
        entrypoint.sh
        systemlibs.sh
        generate_key.sh
      docker-compose-dbonly.yml
      docker-compose-local.yml
      docker-compose-sequential.yml
      Dockerfile
      
      
# Get started
- Clone/download the repo from Github
- Open the terminal/command line to switch the folder to aws-mwaa-local-runner

```
$ cd aws-mwaa-local-runner
```

## Step 1: Building the Docker image
- Build the Docker container image using the following command:

```
$ ./mwaa-local-env build-image
```

## Step 2: Running Apache Airflow
- Runs a local Apache Airflow environment that is a close representation of MWAA by configuration.

```
$ ./mwaa-local-env start
```

## Step 3: Accessing the Airflow UI

- Airflow UI via opening the browser and type the url: http://localhost:8080/.

 * Username: admin
 * Password: test

## Step 4: Setting up the connection to Postgres

- In the top area of Airflow UI, to clikc **Admin** >> **Connections** >> **Add a new record(a plus sign)**
- To add two connections as below and save it.

<img width="1840" alt="image" src="https://user-images.githubusercontent.com/43714137/188956431-fe6447c3-7ddd-44c9-aa31-1ce8d57920bf.png">


<img width="1853" alt="image" src="https://user-images.githubusercontent.com/43714137/188956563-a6dcfa42-3610-4915-87ec-1eab3d35a95b.png">


## Step 5: Activating the dag with two input variables (start date & end date)

- Open another new terminal/command line 
- Get the container id (image: amazon/mwaa-local:2.0.2) using the following commnad 
```
docker ps
```

- Access the container in docker
```
$ docker exec -it <container id> bash
```
- Trigger the dag with start date and end date.

```
$ airflow dags trigger --conf '{"start_date": "<date>", "end_date": "<date>"}' codementor_arc_etl
```

For example.
```
$ airflow dags trigger --conf '{"start_date": "2016-08-03", "end_date": "2016-08-04"}' codementor_arc_etl
```



# Architecture of the Program

<img width="515" alt="image" src="https://user-images.githubusercontent.com/43714137/188958794-16a119df-8fc3-40e7-94d9-c257a14ea142.png">


## **main_process** is mainly composed of three parts:

1. Separately fetch the data from two database with the specific time period via Postgres.
2. Transform data and generate the new column based on the grouping rules of landing url.
3. Load the final result (dateframe) onto the Bigquery.

### - The reason that not splitting the task of **main_process** into three tasks is becuase it avoids passing large amount of reocrds among tasks 
###   although it's easy to track the error log via the Airflow UI.
### - Each step would use try and exception to record the error down once there's any error or exception happens.
### - For further tracking error log, it highly recommends to write the exception log into DynamoDB.

