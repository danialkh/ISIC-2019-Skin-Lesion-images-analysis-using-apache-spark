# Scalable Distributed Pipeline for Clinical Risk Assessment of Skin Lesions

![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![AWS S3](https://img.shields.io/badge/Amazon_S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

## 📋 Project Overview
This project implements a high-scale clinical data mining pipeline using **Apache Spark** and **Amazon S3**. The goal is to quantify the statistical risk of melanoma by intersecting biological identity (age, sex) with lesion location (anatomical site). 

To simulate a production-grade environment, we synthesized a dataset of **2.5 million records** based on the ISIC 2019 dataset to evaluate the performance impact of architectural optimizations.

---

## 🏗️ System Architecture
The pipeline follows a decoupled storage-and-compute architecture:
1.  **Ingestion:** Raw CSV files are ingested from AWS S3.
2.  **Synthesis:** Data is scaled 100x using a Spark `explode` transformation.
3.  **Optimization:** Data is converted to **Apache Parquet** to enable columnar pushdown and vectorized reads.
4.  **Analytics:** Risk factors are calculated through optimized joins and aggregations.



---

## 🛠️ Performance Comparison: Baseline vs. Optimized

This study compares two execution strategies. While the wall-clock time was similar for 2.5 million records, the **Physical Plans** prove the Optimized version is more stable for "Big Data" scales.

| Feature | Non-Optimized (Baseline) | Optimized Version | Verification |
| :--- | :--- | :--- | :--- |
| **Pipeline Time** | **5.21 seconds** | **5.65 seconds** | Slight overhead due to S3 Write |
| **Data Format** | CSV (`Batched: false`) | **Parquet (`Batched: true`)** | Vectorized CPU efficiency |
| **Join Strategy** | Standard Shuffle | **Broadcast Hash Join** | Zero Network I/O during join |
| **Data Handling** | "On-the-Fly" Struggle | **Structured Efficiency** | Pre-cached synthetic data |
| **Shuffle Count** | 2 Global Exchanges | **1 Strategic Repartition** | 50% reduction in data movement |



---

## 🧬 Clinical Risk Matrix Definition
The pipeline transforms raw clinical features into a multi-dimensional risk matrix:

* **Anatomical Site (`anatom_site_general`):** Identifies if specific body areas have higher statistical correlations with malignancy.
* **Sex (`sex`):** Identifies risk disparities based on biological gender.
* **Approximate Age (`age_approx`):** Tracks risk increases correlated with cumulative UV exposure.
* **Risk Factor:** Calculated as `Total Melanoma Cases / Total Cases` for each demographic intersection.

---

## 📉 Data Mining Insights
* **Cumulative Risk:** Findings validated that risk factors increase significantly across 5-year age intervals.
* **Site-Specific Trends:** The analysis identified unique risk profiles for different anatomical locations, providing a "heat map" of vulnerability across the synthesized 2.5 million patient records.

---

## 🚀 Key Takeaways
1.  **Avoid the "On-the-Fly" Struggle:** Generating 2.5 million rows in memory every time is CPU intensive. Pre-calculating and storing in Parquet is the superior architectural choice.
2.  **Shuffle is the Enemy:** By using `broadcast()` for the labels table, we eliminated the primary shuffle stage, ensuring the pipeline can scale to 250M+ rows without crashing.
3.  **Fault Tolerance:** By using S3 for intermediate persistence, we ensure that no data is lost during the shuffle process due to Spark's lineage and task retry mechanisms.

---

## 👤 Author
* **Danial Khayatian** - University of Bologna
* **Course:** Big Data (81932)
* **Academic Year:** 2025/2026

---
*This project was developed as part of the Master's Degree in Computer Science and Engineering.*
