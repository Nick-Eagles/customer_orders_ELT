-- Create the database and schemas for project. I'm running things locally with
-- DBeaver running Postgres SQL

-- Create the database for the project
CREATE DATABASE customer_orders_elt;

-- Create one schema for each layer (using medallion architecture)
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
