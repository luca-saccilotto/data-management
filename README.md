# Business & Technology - Data Management & Processing

This project aims to develop a data management system for a global company. In particular, this organization operates in various countries, but its data is currently spread across multiple sources, making it challenging to access and analyze.

The primary objective of this project is to develop a system that stores the company's data in a database, enabling easy access from a centralized location, and allowing the team to query the relational database for up-to-date business metrics.

## Environment Setup
In this section, the focus is on setting up the development environment. This involved installing the necessary tools and libraries required to run the program. A virtual environment is created to keep the dependencies for the project isolated from the other projects on the machine. This step helps to avoid potential conflicts with other projects.

## Data Processing
A new database is set up and the data extracted from various sources, such as PDFs, an AWS RDS database, RESTful API, JSON, and CSVs. To realize that, three classes are created - `DataExtractor`, `DataProcessor`, and `DatabaseConnector` - which contain methods to extract, transform, and load the data to the database.

These classes are designed to provide an efficient and straightforward process for handling the data from multiple sources and ensure its accuracy and consistency. Overall, this represents a significant step towards centralizing the company's data and enabling the team to access it more efficiently.

## DBMS Schema
Here, the star-based schema of the database is developed, ensuring that the columns are of the correct data types. This design has a simple structure that enables faster querying and aggregation of data. The database schema includes one fact table that contains the primary metrics and several dimension tables with the attributes that provide context to the analysis.

## Querying Data
In this part, the objective is to perform different queries to get information from the database. To answer management questions with some updated metrics, data is pulled from the database using SQL. This means that now the business can start making more data-driven decisions to gain a better understanding of its business, saving time and reducing costs.
