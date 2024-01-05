# Cassandra-Based Weather Data Storage and Analysis

## Objective:
Explore Cassandra's capabilities in storing NOAA weather data, implement a data collection server using gRPC, and analyze the collected data through Spark. Understand Cassandra schema creation involving partition and cluster keys, data movement between Spark and Cassandra, and configure queries for read/write availability tradeoffs.

## Implementation:
Utilize setup.sh script to download and set up necessary files. Deploy the Docker containers for Cassandra using the Dockerfile and docker-compose.yml. Create a Jupyter notebook (p6.ipynb) and a server (server.py) for data collection. Implement gRPC calls for recording temperature data and querying max temperatures. Analyze the collected data using Spark, addressing read/write availability tradeoffs in Cassandra.

### Schema Creation and Data Upload:
Designed Cassandra schema, including station_record type and stations table.
Addressed schema-related questions about station ID correlation, token identification, and vnode token specifics.
Implemented a gRPC server (server.py) for data recording and max temperature queries.
Utilized prepared statements for efficient Cassandra data insertion and querying.
Unzipped records.zip, loaded and rearranged data using PySpark for effective processing.
Iteratively looped over Spark results, making gRPC calls to insert measurements into Cassandra.
Responded to inquiries about maximum temperatures for specific stations.

### Spark Analysis and Cluster Disruption:
Created a Spark temporary view corresponding to the Cassandra stations table.
Computed the average difference between tmax and tmin for specific stations using Spark.
Simulated node failure by killing one Cassandra container.
Analyzed nodetool status output to identify cluster disruptions.
Responded to inquiries about error fields in StationMax RPC calls and RecordTempsRequest RPC calls, addressing different error scenarios.

## Outcome:
The project showcases proficiency in Cassandra schema design, gRPC server implementation, Spark data analysis, and handling node failures in a distributed environment. The comprehensive approach, spanning schema creation, server development, data processing, analysis, and fault tolerance, demonstrates a holistic understanding of Cassandra's capabilities in managing NOAA weather data.



