# Real-Time E-Commerce Data Pipeline

Zuhair Arif - Danish Raza - Shaheer Sabri

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Setup and Installation](#setup-and-installation)
  - [1. Launch Containers](#1-launch-containers)
  - [2. Data Preparation](#2-data-preparation)
  - [3. Deploy Spark Jobs](#3-deploy-spark-jobs)
  - [4. Start Kafka Producer](#4-start-kafka-producer)
  - [5. Configure MongoDB Collection](#5-configure-mongodb-collection)
  - [6. Ingest Data with NiFi](#6-ingest-data-with-nifi)
  - [7. Run Spark Aggregations](#7-run-spark-aggregations)
  - [8. Launch Streamlit Dashboard](#8-launch-streamlit-dashboard)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

## Overview

This project implements a real-time data pipeline for e-commerce analytics. It leverages Docker, Apache Kafka, Apache Spark, MongoDB, Apache NiFi, and Streamlit to ingest, process, analyze, and visualize data in real-time. The pipeline facilitates efficient data flow from ingestion to actionable insights through an interactive dashboard.

## Table of Contents

                               +--------------------+
                              |      Kafka         |
                              |  (ecommerce topic) |
                              +--------------------+
                                       |
                     +-----------------+------------------+
                     |                                    |
            +--------v--------+                +----------v-----------+
            |      NiFi       |                |      Spark           |
            |  Ingests data   |                |  Processes data      |
            |  from Kafka     |                |  from Kafka topic    |
            +--------+--------+                | (ecommerce topic)    |
                     |                         +----------+------------+
                     |                                    |
          +----------v-----------+                        |
          |      MongoDB         |                        |
          |  (Stores ingested    |                        |
          |   data from NiFi)    |                        |
          +----------+-----------+                        |
                     |                                    |
                     |                         +----------v------------+
                     |                         |         HBase         |
                     |                         |  (Stores raw data)    |
                     |                         +----------+------------+
                     |                                    |
                     |                         +----------v------------+
                     |                         |     Kafka             |
                     |                         |  (eda-results topic)  |
                     |                         +----------+------------+
                     |                                    |
                     |                         +----------v------------+
                     |                         |     Streamlit         |
                     |                         |    Dashboards         |
                     |                         |  (Subscribes to       |
                     |                         |   eda-results topic)  |
                     |                         +------------------------+
                     |
              +------v-------+
              |    HBase     |
              | (Optional:   |
              | Additional   |
              | Storage)     |
              +--------------+


## Prerequisites

Ensure you have the following installed on your system:

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [Python 3.8+](https://www.python.org/downloads/)
- [Kaggle CLI](https://github.com/Kaggle/kaggle-api) (for downloading datasets)
- [Git](https://git-scm.com/downloads)

## Setup and Installation

Follow the steps below to set up and run the project. Dataset Link : https://www.kaggle.com/datasets/iyumrahul/flipkartsalesdataset

### 1. Launch Containers

Use Docker Compose to build and run all necessary containers, including Kafka, MongoDB, NiFi, and Spark.

```bash
# Clone the repository
git clone https://github.com/zetomic/ecommerce_pipeline.git
cd ecommerce_pipeline

# Start the containers
docker-compose up -d
```

**Explanation:**

- `docker-compose up -d`: Builds and starts all services defined in the `docker-compose.yml` file in detached mode.

**Note:** Ensure that ports defined in `docker-compose.yml` do not conflict with existing services on your machine.

### 2. Data Preparation

Download the required dataset from Kaggle and execute the data pruning script.

```bash
# Navigate to the data preparation directory
cd data-preparation

# Execute the data pruning script
python dataprune.py
```

**Explanation:**

- `dataprune.py`: Cleans and preprocesses the raw data to prepare it for ingestion.

**Prerequisite:** Ensure you have configured Kaggle API credentials. Refer to [Kaggle API](https://github.com/Kaggle/kaggle-api) for setup instructions.

### 3. Deploy Spark Jobs

Copy the Spark job scripts into the Spark jobs directory within the Spark master container.

```bash
# Copy EDA and HBase scripts into the Spark master container
docker cp eda.py spark-master:/opt/spark-jobs/
docker cp create_hbase_table.py spark-master:/opt/spark-jobs/
docker cp kafka_hbase.py spark-master:/opt/spark-jobs/
```

**Explanation:**

- `docker cp`: Copies files from the host machine to the specified container path.

**Note:** Replace `spark-master` with the actual name of your Spark master container if different.

### 4. Start Kafka Producer

Run the Kafka producer script to ingest data into the Kafka topic.

```bash
# Execute the Kafka producer script
python kafkaproducer.py
```

**Explanation:**

- `kafkaproducer.py`: Reads the preprocessed data and publishes it to the Kafka topic `ecommerce`.

### 5. Configure MongoDB Collection

Create a MongoDB collection based on the columns of the joined dataset.

```bash
# Bash into the MongoDB container
docker exec -it mongodb bash

# Launch Mongo shell
mongosh

# Inside the Mongo shell, create the collection
use ecommerce_db
db.createCollection("joined_dataset")
exit
```

**Explanation:**

- `docker exec -it mongodb bash`: Opens an interactive bash shell inside the MongoDB container.
- `mongo`: Launches the MongoDB shell.
- `use ecommerce_db`: Switches to the `ecommerce_db` database.
- `db.createCollection("joined_dataset")`: Creates a new collection named `joined_dataset`.

### 6. Ingest Data with NiFi

Start the data ingestion process from Kafka to MongoDB using Apache NiFi.

1. **Execute the Kafka Producer:**

    ```bash
    python kafka_producer.py
    ```

    **Explanation:**

    - `kafka_producer.py`: Starts the data ingestion from the Kafka topic `ecommerce` to NiFi.

2. **Access Apache NiFi:**

    Open your web browser and navigate to [https://localhost:8443](https://localhost:8443).

    **Login Credentials:**

    - **Username:** `admin`
    - **Password:** `adminpassword`

3. **Start Processors:**

    - Once logged in, locate your NiFi data flow schema.
    - Click the **Play** button on both processors to initiate the data flow.

**Explanation:**

- NiFi manages the data flow from Kafka to MongoDB, ensuring real-time data ingestion and processing.

### 7. Run Spark Aggregations

Execute the Spark jobs to perform real-time EDA and store results in HBase.

1. **Bash into the Spark Master Container:**

    ```bash
    docker exec -it spark-master bash
    ```

2. **Navigate to the Spark Jobs Directory:**

    ```bash
    cd /opt/spark-jobs
    ```

3. **Execute Spark Submit Commands:**

    **Important:** Execute the `create_hbase_table.py` script **first** to set up the HBase table.

    ```bash
    # Create HBase table
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 create_hbase_table.py

    # Start Kafka to HBase integration
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 kafka_hbase.py

    # Execute EDA Spark job
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 eda.py
    ```

**Explanation:**

- **`create_hbase_table.py`**: Initializes the necessary table in HBase.
- **`kafka_hbase.py`**: Streams data from Kafka to HBase.
- **`eda.py`**: Performs real-time Exploratory Data Analysis (EDA) on the incoming data.

### 8. Launch Streamlit Dashboard

Start the Streamlit application to visualize the EDA results.

```bash
# From the host machine or appropriate environment

# Navigate to the Streamlit application directory
cd path/to/streamlit-app

# Run the Streamlit dashboard
streamlit run streamlit.py
```

**Explanation:**

- `streamlit run streamlit.py`: Launches the Streamlit server and opens the dashboard in your default web browser.

**Note:** Ensure that the `eda_results` Kafka topic is being populated before launching the dashboard to visualize meaningful data.

## Usage

1. **Access the Dashboard:**

    After running the Streamlit application, your default web browser should automatically open the dashboard. If not, navigate to the URL provided in the terminal (typically [http://localhost:8501](http://localhost:8501)).

2. **Interact with the Dashboard:**

    - Use the **Refresh** button to update the analytics based on the latest data ingested.
    - Explore various visualizations representing different EDA metrics.

## Project Structure

```
your-repo-name/
│
├── data-preparation/
│   ├── dataprune.py
│   └── ... (other data prep scripts and raw data)
│
├── spark-jobs/
│   ├── eda.py
│   ├── create_hbase_table.py
│   └── kafka_hbase.py
│
├── docker-compose.yml
│
├── kafkaproducer.py
│
├── kafka_producer.py
│
├── streamlit-app/
│   ├── streamlit.py
│   └── ... (additional Streamlit resources)
│
├── README.md
│
└── ... (other project files)
```

**Description:**

- **`data-preparation/`**: Contains scripts and resources for downloading and preprocessing data.
- **`spark-jobs/`**: Houses Spark job scripts for EDA, HBase table creation, and Kafka integration.
- **`docker-compose.yml`**: Defines the Docker services and configurations for the project.
- **`kafkaproducer.py`**: Script to produce data to the Kafka topic `ecommerce`.
- **`kafka_producer.py`**: Script to ingest data from Kafka to NiFi.
- **`streamlit-app/`**: Contains the Streamlit dashboard application for real-time analytics.
- **`README.md`**: This documentation file.

## Troubleshooting

- **Containers Not Starting:**
  
  - Ensure Docker and Docker Compose are correctly installed.
  - Check for port conflicts and adjust `docker-compose.yml` if necessary.
  - Review container logs for specific error messages:
    
    ```bash
    docker-compose logs -f
    ```

- **Data Not Ingesting into Kafka:**
  
  - Verify that `kafkaproducer.py` is running without errors.
  - Check Kafka broker status and connectivity.

- **NiFi Processors Not Starting:**
  
  - Ensure that NiFi is accessible at [https://localhost:8443](https://localhost:8443).
  - Confirm login credentials are correct.
  - Review NiFi logs for processor-specific issues.

- **Spark Jobs Failing:**
  
  - Ensure all dependencies are correctly specified in the `spark-submit` commands.
  - Check Spark logs within the Spark master container for detailed error messages.

- **Streamlit Dashboard Not Displaying Data:**
  
  - Confirm that Spark jobs are running and populating the `eda_results` Kafka topic.
  - Ensure network connectivity between Streamlit and Kafka.

## Contributing

Contributions are welcome! Please follow the steps below to contribute to the project:

1. **Fork the Repository**

2. **Create a New Branch**

    ```bash
    git checkout -b feature/YourFeatureName
    ```

3. **Make Your Changes**

4. **Commit Your Changes**

    ```bash
    git commit -m "Add your descriptive commit message"
    ```

5. **Push to Your Fork**

    ```bash
    git push origin feature/YourFeatureName
    ```

6. **Open a Pull Request**

    - Navigate to the original repository and create a pull request from your forked branch.

