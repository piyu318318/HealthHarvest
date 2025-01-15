CREATE DATABASE IF NOT EXISTS HealthHarvestDatabase;
USE HealthHarvestDatabase;

CREATE TABLE HealthHarvest (
    Patient_Name VARCHAR(200),
    Patient_Age INT,
    Patient_Gender VARCHAR(50),
    Blood_Type VARCHAR(200),
    Medical_Condition TEXT,
    Date_of_Admission DATE,
    Assigned_Doctor VARCHAR(255),
    Insurance_Provider VARCHAR(255),
    Billing_Amount DECIMAL(10, 2),
    Room_Number INT,
    Admission_Type VARCHAR(50),
    Discharge_Date DATE,
    Prescribed_Medication TEXT,
    Test_Results TEXT,
    city VARCHAR(100)
);