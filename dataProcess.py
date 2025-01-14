from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np

spark = SparkSession.builder.appName("read 1 crore data").config("spark.jars",
                                                                 r"C:\Users\dixit\PycharmProjects\pythonProgram\HealthHarvest\mysqlJar\mysql-connector-j-9.1.0.jar").getOrCreate()

filepath = r"C:/Users/dixit/PycharmProjects/pythonProgram/mydata.csv"

rdd = spark.read.csv(filepath, header=True, inferSchema=True)

rdd = rdd.withColumnRenamed("Name", "Patient_Name") \
    .withColumnRenamed("Age", "Patient_Age") \
    .withColumnRenamed("Gender", "Patient_Gender") \
    .withColumnRenamed("Blood Type", "Blood_Type") \
    .withColumnRenamed("Medical Condition", "Medical_Condition") \
    .withColumnRenamed("Date of Admission", "Date_of_Admission") \
    .withColumnRenamed("Doctor", "Assigned_Doctor") \
    .withColumnRenamed("Insurance Provider", "Insurance_Provider") \
    .withColumnRenamed("Billing Amount", "Billing_Amount") \
    .withColumnRenamed("Room Number", "Room_Number") \
    .withColumnRenamed("Admission Type", "Admission_Type") \
    .withColumnRenamed("Discharge Date", "Discharge_Date") \
    .withColumnRenamed("Medication", "Prescribed_Medication") \
    .withColumnRenamed("Test Results", "Test_Results")


#piechart diabetes patient accroding to the ages
filtered_df = rdd.filter(rdd['Medical_Condition'] == 'Diabetes')
gender_counts = filtered_df.groupBy("Patient_Gender").count().collect()
labels = [row['Patient_Gender'] for row in gender_counts]
sizes = [row['count'] for row in gender_counts]
plt.figure(figsize=(6, 6))
plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, colors=['blue', 'pink'])
plt.title('Diabetes Patient Gender Distribution')
plt.show()


#ba chart on obesity
ObesityDF = rdd.filter((rdd['Medical_Condition'] == 'Obesity') & (rdd['Patient_Age'] > 25))
gender_counts = (ObesityDF.groupBy("Patient_Gender").count().collect())
labels = [row['Patient_Gender'] for row in gender_counts]
counts = [row['count'] for row in gender_counts]
plt.figure(figsize=(8, 6))
plt.bar(labels, counts, color=['blue', 'pink'])
plt.title('Obesity Cases by Gender whose age is less than 25')
plt.xlabel('Gender')
plt.ylabel('Count')
plt.show()


# databaseName = "pysparkproject"
# jdbc_url = f"jdbc:mysql://localhost:3306/{databaseName}"
# properties = {"user":"root","password":"root123","driver":"com.mysql.cj.jdbc.Driver"}
# rdd.write.jdbc(url=jdbc_url, table="patient_records", mode = "append", properties=properties)
