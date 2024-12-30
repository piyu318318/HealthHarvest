from pyspark.sql import SparkSession
import matplotlib.pyplot as plt


spark = SparkSession.builder.appName("read 1 crore data").config("spark.jars", r"C:\Users\dixit\PycharmProjects\pythonProgram\HealthHarvest\mysqlJar\mysql-connector-j-9.1.0.jar").getOrCreate()

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


filtered_df = rdd.filter((rdd["Medical_Condition"] == "Diabetes") & (rdd["Patient_Age"] < 30 ))
Only_Age_RDD =  filtered_df.select("Patient_Age")
Only_Age_RDD.show(4)

age_data = Only_Age_RDD.collect()

ages = [row["Patient_Age"] for row in age_data]

x_axis = range(len(ages))

# Plot the data
plt.figure(figsize=(10, 6))
plt.plot(x_axis, ages, marker="o", linestyle="-", color="b", label="Patient Age")

plt.xlabel("Record Index")
plt.ylabel("Patient Age")
plt.title("Patient Age for Diabetes Patients Under 30")
plt.legend()
plt.grid(True)

plt.show()




# databaseName = "pysparkproject"
# jdbc_url = f"jdbc:mysql://localhost:3306/{databaseName}"
# properties = {"user":"root","password":"root123","driver":"com.mysql.cj.jdbc.Driver"}
# rdd.write.jdbc(url=jdbc_url, table="patient_records", mode = "append", properties=properties)