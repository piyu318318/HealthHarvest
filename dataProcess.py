from pyspark.sql import SparkSession
from matplotlib.ticker import FuncFormatter
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import functions as F


spark = SparkSession.builder.appName("read 1 crore data").config("spark.jars",
                                                                 r"C:\Users\dixit\PycharmProjects\pythonProgram\HealthHarvest\mysqlJar\mysql-connector-j-9.1.0.jar").getOrCreate()

filepath = r"C:/Users/dixit/PycharmProjects/pythonProgram/20kdata.csv"

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
    .withColumnRenamed("Test Results", "Test_Results") \
    .withColumnRenamed("city", "city")



#piechart diabetes patient accroding to the ages
filtered_df = rdd.filter(rdd['Medical_Condition'] == 'Diabetes')
gender_counts = filtered_df.groupBy("Patient_Gender").count().collect()
labels = [row['Patient_Gender'] for row in gender_counts]
sizes = [row['count'] for row in gender_counts]
plt.figure(figsize=(6, 6))
plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, colors=['blue', 'pink'])
plt.title('Diabetes Patient Gender Distribution')
plt.show()



#plot the bar graph for number of cities and patient avaliable in the coresponding city
city_patient_counts = rdd.groupBy("city").agg(F.count("Patient_Name").alias("Patient_Count"))
city_patient_counts_list = city_patient_counts.collect()
cities = [row['city'] for row in city_patient_counts_list]
patient_counts = [row['Patient_Count'] for row in city_patient_counts_list]
plt.figure(figsize=(10, 6))
plt.bar(cities, patient_counts, color='skyblue')
plt.xlabel('City')
plt.ylabel('Number of Patients')
plt.title('Number of Patients by City')
plt.xticks(rotation=45)
plt.show()



#plot the line graph
data = rdd.collect()
pandas_df = pd.DataFrame(data, columns=rdd.columns)
pandas_df['Date_of_Admission'] = pd.to_datetime(pandas_df['Date_of_Admission'])
pandas_df['Year_of_Admission'] = pandas_df['Date_of_Admission'].dt.year
billing_per_year = pandas_df.groupby('Year_of_Admission')['Billing_Amount'].sum()
def currency_format(x, pos):
    return '₹{:,.2f}'.format(x)
plt.figure(figsize=(10, 6))
billing_per_year.plot(kind='line', marker='o', color='blue')
plt.title('Total Billing Amount per Year')
plt.xlabel('Year')
plt.ylabel('Total Billing Amount')
plt.gca().yaxis.set_major_formatter(FuncFormatter(currency_format))
plt.grid(True)
plt.xticks(billing_per_year.index)
plt.show()




# databaseName = "pysparkproject"
# jdbc_url = f"jdbc:mysql://localhost:3306/{databaseName}"
# properties = {"user":"root","password":"root123","driver":"com.mysql.cj.jdbc.Driver"}
# rdd.write.jdbc(url=jdbc_url, table="patient_records", mode = "append", properties=properties)
