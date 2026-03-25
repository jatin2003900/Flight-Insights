# Big Data Analytics on Air Flights

#### Keywords

`Big Data Analytics`, `Apache Spark`, `PySpark`, `Data Engineering`, `Streamlit`, `Interactive Dashboards`, `Aviation Data`, `Data Visualization`, `Cloud-Scale Data Processing`.

This project focuses on the **analysis and interactive exploration of large-scale aviation data** using **Big Data technologies**. The objective is to extract meaningful insights from millions of flight records by leveraging distributed data processing and real-time visualization techniques.

The system analyzes domestic US flights data from the **Reporting Carrier On-Time Performance** dataset, restricted to the year **2013**, comprising over **6 million records** with detailed information on schedules, delays, cancellations, and diversions. The backend is powered by **Apache Spark (PySpark)**, while the frontend is implemented using **Streamlit** to provide an interactive, user-friendly analytical dashboard.

This project was developed as part of the coursework exam for the *Models and Techniques for Big Data* course during the academic year **2022/2023**. 

---

### Key Features

* **Large-Scale Data Processing**: Efficient handling of millions of flight records using Apache Spark.
* **Interactive Analytics**: Real-time exploration through a Streamlit-based web interface.
* **Geospatial Visualization**: Map-based representation of routes, delays, and diversions.
* **Performance Optimization**: Use of caching mechanisms to reduce computation time.
* **User-Driven Queries**: Dynamic flight search by date, time, and airports.

---

### Project Structure

* **Data Processing Layer**

  * Unified PySpark DataFrame created by merging monthly CSV files
  * Extensive preprocessing: type casting, timestamp conversion, and data normalization
  * Caching of frequently accessed DataFrames to optimize performance

* **Geolocation Preprocessing**

  * Offline computation of airport coordinates using **OpenStreetMap (Nominatim)**
  * Resolution of origin–destination routes for map-based visualization

* **Interactive Dashboard (Streamlit)**

  * **Homepage**: Dataset overview, most frequent aircraft, longest and most-used routes
  * **Monthly Flights Analysis**: Flight volume trends and month-over-month comparisons
  * **Delay Analysis**: Identification of routes with the highest average and relative delays, including cause breakdown
  * **Diversion Analysis**: Statistics on diverted flights and destination recovery rates
  * **Flight Search**: Detailed lookup of individual flights with schedule adherence, delay causes, and route maps

* **Visualization Tools**

  * **PyDeck ArcLayer** for rendering flight routes on maps
  * Metrics, sliders, and pie charts for intuitive data interpretation

---

### Report

* **Technical Report (Italian)**:

  * Detailed description of the dataset and preprocessing steps
  * Explanation of Spark transformations, actions, and caching strategies
  * Design and implementation of the Streamlit interface
  * Extensive visual analysis of flight behavior, delays, and anomalies
