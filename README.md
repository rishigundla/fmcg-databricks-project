# FMCG Data Engineering Project — Unifying Analytics on Databricks

This project implements an **End-to-End Data Engineering Pipeline** on the **Databricks Lakehouse Platform** to unify the analytics for two FMCG companies: **Atlikon** and its newly acquired subsidiary, **Sportsbar**. It leverages the **Medallion Architecture**, **Delta Lake**, and **Databricks Workflow** to deliver reliable, incremental, and business-ready data for a unified BI Dashboard.

---

## 🎯 Project Goals & Business Solution

### Business Overview

* **Atlikon (Parent Company):** An established manufacturer of sports equipment (e.g., cricket, badminton). It has a mature analytical system (OLAP) already leveraging the Medallion Architecture for reliable data.
* **Sportsbar (Subsidiary):** A smaller company focused on energy drinks and nutrition. It lacks a proper OLAP system and currently relies on direct OLTP data dumps for simple Excel-based dashboards.

### Management Expectation

The management requires a **streamlined, single dashboard** to analyze the aggregated performance of *both* companies, mandating the integration of Sportsbar's data into Atlikon's existing analytical architecture.

### Data Engineering Solution

The Data Engineering team's goal is to **onboard Sportsbar's data** into the established **Medallion Architecture**. This involves building a dedicated incremental pipeline for Sportsbar to cleanse and transform its raw transactional data to the same standard as Atlikon's, enabling the Data Analyst team to build the required unified BI Dashboard.

---

## 🌟 Project Highlights

* **Dual-Company Integration:** Successfully merged data from two distinct entities (Atlikon & Sportsbar) into a unified Star Schema (Gold Layer).
* **Incremental Processing:** Implemented a robust backfilling (initial 6 months) and a daily incremental update pipeline using Delta Lake.
* **Automated Orchestration:** Deployed **Databricks Workflow** jobs, configured to trigger automatically upon file arrival in **ADLS Gen2**, ensuring near real-time data freshness.
* **Reliable Data:** Leveraged **Delta Lake** features (e.g., UPSERTs for DML) to ensure data quality and atomicity across all layers.
* **Unified Reporting:** Delivered aggregate analytics and key performance indicators (KPIs) through a single **Databricks BI Dashboard**.
* **Governance:** Utilized a structured catalog and schema model for clear data governance.

---

## 🧭 Technical Architecture & Data Flow

This solution implements a specialized Medallion flow to accommodate the different maturity levels of the two companies:

| Layer | Processing Logic | Data Sources & Status |
| :--- | :--- | :--- |
| **01\_Bronze** | **Ingestion:** Raw files loaded with minimal schema enforcement. | **Sportsbar:** Raw orders, customers, products, etc. |
| **02\_Silver** | **Cleansing & Standardization:** Data quality checks, deduplication, and necessary transformations applied. | **Sportsbar:** Cleansed data from Bronze layer. **Atlikon:** Clean data is loaded directly into Silver (as per assumption of mature source). |
| **03\_Gold** | **Curation:** Merging the cleaned data from both companies to build the final **Star Schema** tables. | **Unified Data:** Merged Atlikon and Sportsbar data ready for consumption. |
| **View Layer** | **Final Reporting View:** `vw_sales_analytics` is created over the Gold tables for optimal BI tool performance. | Ready for BI Dashboard. |
| **Orchestration** | **Databricks Workflow:** Jobs are triggered by file arrival in ADLS Gen2 for incremental updates. | Automated refresh of all layers and dashboard. |

![Dashboard Screenshot](./assets/fmcg_databricks_project_architecture.drawio.png)

---

## ⚙️ Orchestration: Databricks Workflow & Automation

The entire pipeline is automated using Databricks Workflow, moving beyond manual execution to establish a production-ready, event-driven process.

The Workflow job is specifically configured to handle **daily incremental data** from the Sportsbar company:

* **Trigger Mechanism:** The workflow is set to trigger automatically upon the **arrival of the daily orders file** in the designated landing directory within **ADLS Gen2** (Azure Data Lake Storage Gen2).
* **End-to-End Refresh:** As soon as the daily orders file is placed by the user, the Databricks Workflow jobs are triggered.
* **Job Sequence:** The workflow executes the notebooks sequentially, ensuring all layers (Bronze, Silver, Gold), the analytical view (`vw_sales_analytics`), and the final BI Dashboard are refreshed with the latest data. This guarantees data freshness and consistency across the entire Lakehouse.

---

## 🧰 Tech Stack

| Category | Tools / Tech Used |
| :--- | :--- |
| **Data Platform** | **Databricks** |
| **Storage** | Delta Lake, Cloud Storage (**ADLS Gen 2**) |
| **Architecture** | Medallion (Bronze, Silver, Gold) |
| **Languages** | **PySpark**, **SQL** |
| **Orchestration** | **Databricks Workflow** (Triggered on File Arrival) |
| **Visualization** | Databricks BI Dashboard |
| **Data Model** | Star Schema |

---

## 🧱 Data Model (Gold Layer)

The Gold layer uses a **Star Schema** centered around the `fact_orders` table to simplify cross-company analytics.

### Catalog & Schemas
* **Catalog:** `FMCG`
* **Schemas:** `fmcg.01_bronze`, `fmcg.02_silver`, `fmcg.03_gold`

| Table Type | Table Name | Purpose |
| :--- | :--- | :--- |
| **Fact Table** | `fact_orders` | Contains all unified transactional data from both companies. |
| **Dimension Table** | `dim_customers` | Contains unified customer master data. |
| **Dimension Table** | `dim_products` | Contains unified product hierarchy (including both sports gear and nutrition). |
| **Dimension Table** | `dim_gross_price` | Contains unified gross price data. |
| **Dimension Table** | `dim_calendar` | Custom-built date table for advanced time-based analysis. |
| **Reporting View** | `vw_sales_analytics` | Final aggregated view to feed the BI Dashboard. |

---

## 💾 Data Folder Structure

📁 **`/datasets`**

The data folder organizes files based on the company, load type, and destination layer:

| Directory | Data Content | Destination Layer/Purpose |
| :--- | :--- | :--- |
| `atlikon_company/full_load` | Historical CSV files for initial backfill. | **Silver Layer** (Full Load) |
| `sportbars_company/full_load` | Raw CSV files for the 6-month backfill (July - Nov 2025). | **Bronze Layer** (Full Load) |
| `sportbars_company/incremental_load/orders` | Daily order files for ongoing updates (Dec 2025 onwards). | **Trigger for Databricks Workflow** |

| File Name | Description |
| :--- | :--- |
| `customers.csv` | Customer master data. |
| `products.csv` | Product master data. |
| `gross_prices.csv` | Price history. |
| `orders.csv` | Fact table containing transactional details. |

---

## 📓 Project Notebooks & Workflow Steps

The core logic is implemented in modular PySpark/SQL notebooks, which are orchestrated by the Databricks Workflow job.

| Step | Notebook / Action | Description |
| :--- | :--- | :--- |
| **01** | `dim_date_table_creation.py` | Creates and populates the static **`dim_calendar`** table. |
| **02** | `dim_customers_data_processing.py` | ETL for `dim_customers`: Ingestion, Cleaning, and merging into the Gold Layer. |
| **03** | `dim_products_data_processing.py` | ETL for `dim_products`: Ingestion, Cleaning, and merging into the Gold Layer. |
| **04** | `dim_gross_price_data_processing.py` | ETL for `dim_gross_price`: Ingestion, Cleaning, and merging into the Gold Layer. |
| **05** | `fact_order_data_processing.py` | ETL for `fact_orders`: Handles initial backfill and **incremental UPSERTs** for daily orders. |
| **06** | `vw_sales_analytics.txt` | Creates the final optimized reporting view over the Gold Star Schema. |
| **07** | `FMCG Sales Analytics dashboard` | **Refresh** the final BI Dashboard in Databricks. |

All notebooks are included in the [`notebooks/`](./notebooks) directory for reference.

---

## 📊 BI Dashboard

The **FMCG Sales Analytics dashboard** provides the single source of truth for the management, visualizing the aggregated performance of both Atlikon and Sportsbar.



### Key Metrics & Features
* **Key Metrics:** Total Revenue, Total Profit, Total Quantity Sold.
* **Revenue Trends:** Monthly and Quarter-over-Quarter Revenue analysis.
* **Distribution:** Product and Category-wise Revenue Distribution.
* **Real-time:** Near real-time insights based on the curated Gold layer data, automatically refreshed by the Databricks Workflow.

---

## 📚 Key Learnings

* Designing and implementing a complex **Dual-Source Medallion Architecture** pipeline.
* Implementing incremental data processing with **Databricks Workflow** and **Delta Lake UPSERTs**.
* Building a **Unified Star Schema** to merge and reconcile data from two different business units.
* Applying necessary **business transformations** (e.g., currency conversion, unit standardization) in the Silver layer.
* Delivering analytical insights using the native **Databricks BI Dashboard**.

---

## 👨‍💻 Author

**Rishikesh Gundla**  
📊 Senior BI Engineer | 📍 India  
🔗 [LinkedIn](https://www.linkedin.com/in/rishikeshgundla/)
