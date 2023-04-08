# Toronto Bicycle Data Engineering

- This is my submission for the [final project](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_7_project/README.md) of the datatalks club data engineering zoomcamp

## Project Description
- The goal of this project is to create a comparative analytical analysis of a few bike share programs in Canada.
- Given data availability, these programs will be those located in 3 cities: Toronto, Montreal and Vancouver. The initial focus of this analysis will be the city of Toronto, with successive implementations to include the other two cities as well.
- For the city of Toronto, the project will consist of 2 main pipelines:
    - a batch pipeline that runs monthly and sources data from ridership data
    - a near real time pipeline that runs every 5 minutes and sources data from the gbfs API
    - See data sources below for more information

## Architecture
- Initially, this project will deal with data in batch as the primary data sources we are dealing with get updated monthly
- The bike share programs do seem to have real-time data feeds for the bike locations, but we will revisit that at a later time
- We are going to create 2 pipeline flows: dev and prod
    - dev:
        - the dev pipeline will deploy using locally hosted components
        - no terraform, postgres, prefect orion, dbt core, metabase
    - prod:
        - the prod pipeline will deploy using mostly hosted components in gcp
        - terraform, prefect cloud, dbt cloud, gcs, bq, looker

## Data sources
### Toronto
- Ridership data: https://open.toronto.ca/dataset/bike-share-toronto-ridership-data/
- Neighbourhood profiles:
    - info: https://www.toronto.ca/city-government/data-research-maps/neighbourhoods-communities/neighbourhood-profiles/
    - data: https://open.toronto.ca/dataset/neighbourhood-profiles/

### Montreal:
- data: https://bixi.com/en/open-data

### Vancouver:
- Program developed by mobi bikes
- data: https://www.mobibikes.ca/en/system-data

## Deployment instructions

## Future considerations:
- There appears to be a "mobility data" protocol called GBFS that consolidates data from many cities with mobility programs and various vehicle types
- One possible alternative to the above is to build pipelines using the gbfs API: https://github.com/MobilityData/gbfs
- This is a handy lookup that shows all cities with shared bike (and other mobility) data: https://github.com/MobilityData/gbfs/blob/master/systems.csv

## Evaluation Criteria
- Aiming to hit all parts of the [Evaluation Criteria](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_7_project/README.md):
* Problem description
    * 0 points: Problem is not described
    * 1 point: Problem is described but shortly or not clearly 
    * 2 points: Problem is well described and it's clear what the problem the project solves
* Cloud
    * 0 points: Cloud is not used, things run only locally
    * 2 points: The project is developed in the cloud
    * 4 points: The project is developed in the cloud and IaC tools are used
* Data ingestion (choose either batch or stream)
    * Batch / Workflow orchestration
        * 0 points: No workflow orchestration
        * 2 points: Partial workflow orchestration: some steps are orchestrated, some run manually
        * 4 points: End-to-end pipeline: multiple steps in the DAG, uploading data to data lake
    * Stream
        * 0 points: No streaming system (like Kafka, Pulsar, etc)
        * 2 points: A simple pipeline with one consumer and one producer
        * 4 points: Using consumer/producers and streaming technologies (like Kafka streaming, Spark streaming, Flink, etc)
* Data warehouse
    * 0 points: No DWH is used
    * 2 points: Tables are created in DWH, but not optimized
    * 4 points: Tables are partitioned and clustered in a way that makes sense for the upstream queries (with explanation)
* Transformations (dbt, spark, etc)
    * 0 points: No tranformations
    * 2 points: Simple SQL transformation (no dbt or similar tools)
    * 4 points: Tranformations are defined with dbt, Spark or similar technologies
* Dashboard
    * 0 points: No dashboard
    * 2 points: A dashboard with 1 tile
    * 4 points: A dashboard with 2 tiles
* Reproducibility
    * 0 points: No instructions how to run code at all
    * 2 points: Some instructions are there, but they are not complete
    * 4 points: Instructions are clear, it's easy to run the code, and the code works


## Going the extra mile 

If you finish the project and you want to improve it, here are a few things you can do:

* Add tests
* Use make
* Add CI/CD pipeline 

This is not covered in the course and this is entirely optional.

If you plan to use this project as your portfolio project, it'll 
definitely help you to stand out from others.

> **Note**: this part will not be graded. 