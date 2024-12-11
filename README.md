# **Bulk File Handling, Conversion, and SQL Querying on Credit Card Fraud Detection Data**

## **Objective**  
The goal of this task is to perform bulk file handling, file conversion, and SQL querying on the Credit Card Fraud Detection dataset from Kaggle using Apache Spark with Python (PySpark). The task also includes creating a new column based on a mathematical formula to test logical skills.

---
## **Task Description**

### **1. Data Acquisition**  
- Downloaded the Credit Card Fraud Detection dataset from Kaggle.  
- Simulated bulk file handling by duplicating the dataset into multiple files with different names.
- 
### **2. Bulk File Handling**  
- Loaded all the CSV files into a single Spark DataFrame.  
- Since the Kaggle dataset contains only one file, duplicate files were created with different names.  
- Data deduplication was performed to ensure unique records before loading into the DataFrame.
- 
### **3. Data Exploration**  
Performed exploratory data analysis on the DataFrame, which includes:  
- Checking the shape of the dataset.  
- Checking for null or missing values.  
- Generating descriptive statistics (mean, median, mode, etc.) using `df.describe()`.
---
### **4. Data Cleaning**  
Cleaned the dataset to prepare it for the ETL process:  
- Handled missing values (if any).  
- Renamed columns for consistency, if necessary.  
---

### **5. Data Transformation**  
- **Created a new column**:  
  - **`NormalizedAmount`**: Values from the `Amount` column were normalized by subtracting the mean and dividing by the standard deviation.  
---

### **6. Mathematical Transformation**  
- **Created a new column**:  
  - **`AmountLog`**: Contains the natural logarithm of the `Amount` column plus one (to handle zero values). This transformation helps reduce the impact of extreme values or outliers.
---
### **7. File Conversion**  
- Converted the cleaned and transformed DataFrame from CSV to Parquet format for efficient querying and storage.

---

### **8. SQL Querying**  
The DataFrame was registered as a temporary SQL view to enable SQL-based querying. Queries performed include:  
1. What is the **average normalized amount** in fraudulent transactions?  
2. What is the **maximum normalized amount** in non-fraudulent transactions?  
3. What is the **average of the `AmountLog` column** for fraudulent and non-fraudulent transactions?
