# Asteroid Trajectory Analysis 🚀

Asteroid Trajectory Analysis is a data engineering project that ingests, processes, and analyzes asteroid trajectory data using NASA's NEO API. The project supports batch and real-time data pipelines, enabling comprehensive insights into asteroid movements, historical trajectories, and potential risks.

---

## 🌟 **Features**
- **Real-Time Data Ingestion**: Uses Apache Airflow to fetch daily asteroid trajectory data.
- **Batch Data Processing**: Processes 10 years of historical asteroid data.
- **Data Transformation**: Cleanses and aggregates data using PySpark.
- **Data Storage**: Stores both raw and processed data in AWS S3.
- **Querying**: Uses AWS Athena to generate actionable insights.
- **Lambda Automation**: AWS Lambda checks for new files in S3 and updates the Athena table automatically with the latest records.
- **Visualization**: Power BI dashboard visualizes key metrics like trajectory paths and hazardous asteroids.
- **Future Scope**: Incorporates predictive analytics using machine learning.

---

## 🏗️ **Project Architecture**

Here’s how the system is designed:

1. **Data Ingestion**:
   - Historical data is fetched from APIs and loaded into AWS S3.
   - Real-time data ingestion is managed using Apache Kafka and Airflow.

2. **Data Transformation**:
   - PySpark scripts process, clean, and aggregate data.
   - Aggregated data is stored in S3 for querying.

3. **Automated Table Updates**:
   - AWS Lambda is triggered when a new file is uploaded to S3.
   - Lambda runs an AWS Glue crawler or directly updates the Athena table with the new schema or data.

4. **Data Analytics**:
   - AWS Athena queries aggregated data stored in S3.
   - Power BI connects to Athena for visualization.

5. **Pipeline Automation**:
   - Airflow DAGs schedule and monitor all ETL tasks.

---

### **Architecture Diagram**

![diagram](https://github.com/user-attachments/assets/9811b86e-63c9-43ec-85e9-0f705d789145)


---

## 📊 **Power BI Dashboard**

The project includes an interactive Power BI dashboard to visualize asteroid trajectory and risks.

### **Dashboard Features**:
- **Asteroid Path Visualization**: Tracks trajectories of asteroids over time.
- **Hazardous Asteroids**: Highlights potentially dangerous objects.
- **Historical Trends**: Analyzes asteroid activity over 10 years.
- **Real-Time Insights**: Incorporates daily data from NASA's API.

### **Preview**:

![dashboard](https://github.com/user-attachments/assets/ab0a64b6-98d1-4fd9-be28-aa542592c424)

### **How to Use**:
1. Download the `.pbix` file from the `dashboard/` folder in this repository.
2. Open it in [Power BI Desktop](https://powerbi.microsoft.com/desktop/).
3. Set up credentials for Athena to load live data.
4. Explore insights and visualizations.

---

## 📂 **Folder Structure**

```plaintext
Asteroid-Trajectory-Analysis/
├── airflow_dags/         # Airflow DAGs for scheduling tasks
├── data/                 # Sample data or test files
├── scripts/              # PySpark and Kafka scripts
├── dashboard/            # Power BI .pbix file
├── requirements.txt      # Python dependencies
├── README.md             # Project documentation
└── .gitignore            # Ignore unnecessary files
