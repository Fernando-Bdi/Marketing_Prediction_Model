# MongoDB Data Analysis Project

## Overview
This project explores how MongoDB can be used to store and analyze data from a CSV file.  
Instead of using MySQL (structured tables), I chose MongoDB, a NoSQL database that stores data as flexible documents.

## 🗄️ Why MongoDB?
- MySQL: Structured data (tables)  
- MongoDB: Semi-structured/unstructured data (documents)  
- Easy CSV/JSON upload through MongoDB Compass  
- More flexibility when working in Jupyter Notebook

## Project Setup
I created a custom *Mongo_DB* class to:
- Connect to the MongoDB cluster  
- Load data efficiently  
- Work directly with DataFrames  

This method is faster than manually importing and cleaning data.
