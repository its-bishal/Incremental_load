

# Incremental Load

This is an automation project for loading files from an SQL database to a Hadoop server. The project is designed to handle incremental data loading efficiently and effectively.

## Tools Used

### MySQL
MySQL is used as the source database from which data is extracted. It is a widely used relational database management system that supports a wide range of applications.

### PySpark
PySpark, the Python API for Apache Spark, is used for data processing. It allows for distributed data processing and provides APIs for working with structured and semi-structured data.

### Hadoop
Hadoop is the destination storage system. It is a distributed storage and processing framework that handles large data sets across clusters of computers.

## Project Overview

This project automates the process of incrementally loading data from a MySQL database to a Hadoop server. The steps involved include:

1. **Extract Data**: Extract incremental data from MySQL based on a specified condition (e.g., last modified timestamp).
2. **Transform Data**: Use PySpark to process and transform the extracted data as needed.
3. **Load Data**: Load the transformed data into Hadoop for storage and further analysis.

## Installation

To set up the project, follow these steps:

1. **Clone the Repository**
2. **create connection for your database and hadoop server**
3. **Install Required Libraries**: Install PySpark, MySQL Connector, and other required libraries using pip
4. **Configure Environment Variables**: Set environment variables for MySQL connection, Hadoop connection, and other
5. **Run the Script**: Run the Python script to execute the incremental load process

