![Python](https://img.shields.io/badge/Python-3.12.4-blue)
![Pandas](https://img.shields.io/badge/Pandas-Data%20Processing-green)
![Status](https://img.shields.io/badge/Project-Completed-success)


# Modular SQL-Inspired ETL Pipeline (File-Based Data)

<br>

## 📌 Executive Overview

This project implements a reusable Extract module that accepts any CSV, Excel, or Parquet file. This module converts the input data into a Python dictionary, which is then passed to the specific Transformation module. The Transformation module applies business rules by calling a common library that replicates standard SQL functions in a file-based environment using **Pandas**. Finally, the processed data is passed to the reusable Load module, which converts the output into an Excel file and applies versioning. This architecture ensures a clean separation of responsibilities between the components.
<br>

> ⚠️ The purpose of this project is to develop familiarity with Pandas by applying SQL-style business rules to file-based data sources.
---
<br>


### 🏗️ Architecture  
This separation of responsibilities allows the reuse of the Extract and Load class across each pipeline, while the transformation logic invokes functions from the SQL-style Pandas library.

<img width="852" height="599" alt="image" src="https://github.com/user-attachments/assets/a6833474-346a-4a55-a8a7-b9959bc44bd0" />


### 💼 Pipelines Work Flow  
During execution, three scenarios run independently, each calling the **Extract step** with different file types. The **Transformation step** applies SQL-style Pandas functions for each business need, and the **Load step** generates and versions the final report.

<img width="1172" height="1060" alt="image" src="https://github.com/user-attachments/assets/fb5b2d5e-3195-4ed0-879d-17d4e95203a2" />

---
<h2 align="center">💼 Business Use Cases</h2>

<table width="100%">
<thead>
<tr style="background-color:#f2f2f2;">
<th align="center">🏢 Product Rentals</th>
<th align="center">📈 Marketing Campaign</th>
<th align="center">🏥 Hospital Billing</th>
</tr>
</thead>

<tbody>

<tr style="background-color:#fafafa;">
<td colspan="3" align="center"><b>📥 Input Files</b></td>
</tr>

<tr>
<td valign="top">

<b>Rentals_data.parquet</b>

Marketplace  
Customer Site ID  
Product  
Monthly Rental Amount  

<br>

<b>Segments.parquet</b>

Maps products into segment groups:

• Seg 1–3  
• Seg 4–6

</td>

<td valign="top">

<b>Campaign datasets</b>

tira_campaign_data.csv  
nykaa_campaign_data.csv  
purplle_campaign_data.csv  

<br>

Campaign_Type  
Language  
Revenue  
Date

</td>

<td valign="top">

<b>hospital_billing_data.xlsx</b>

<br>

<b>age_ranges.xlsx</b>

Maps ages into groups:

• Child  
• Adult  
• Elderly

</td>
</tr>

<tr style="background-color:#fafafa;">
<td colspan="3" align="center"><b>📏 Business Rules Applied</b></td>
</tr>

<tr>

<td valign="top">

• Include only rentals ≥ $25 per month  

• Count how many units each customer rents  

<b>Customer Classification</b>

1–2 units  
3–5 units  
6+ units  

• Pivot Segment values (Seg 1–3, Seg 4–6) into columns  

• Group pivot results by MARKET_PLACE and Category  

• Add subtotal rows by MARKET_PLACE  

• Add grand total row across MARKET_PLACE  

• Order final dataset by MARKET_PLACE and Category hierarchy  

</td>

<td valign="top">

• Normalize column names  

• Add Brand column (Nykaa, Purplle, Tira)  

• Union datasets from all brands into one  

• Convert Date column to date format  

• Extract Month from Date  

• Extract Year from Date  

• Convert Revenue column to numeric  

• Filter records where Revenue ≥ 1000  

• Select columns for analysis  
(Brand, Campaign_Type, Language, Revenue, Date, Month, Year)

• Aggregate revenue by Brand, Year, Campaign_Type  

• Sort final dataset by Brand, Year, Campaign_Type, SumRevenue  

</td>

<td valign="top">

• Normalize column names in all input datasets  

• Convert BillAmount column to numeric type  

• Join billing data with AgeRange reference table using AgeRangeID (inner join)

• Filter records where BillAmount ≥ 1000  

• Select columns  
Province, PatientID, AgeRangeLabel, Hospital, BillAmount  

• Create Bill_Amt_Cat category column based on BillAmount  

1.0–5k → 1000–5000  
2.5k–10k → 5001–9999  
3.10k+ → ≥ 10000  

• Pivot AgeRangeLabel values (Child, Adult, Elderly) into columns  

• Group pivot by Province and Bill_Amt_Cat  

• Add subtotal rows by Province  

• Add grand total row across all Provinces  

• Order final results by Province and Bill_Amt_Cat hierarchy  

</td>

</tr>

</tbody>
</table>

---
<br>

## 🧠 Skills Demonstrated

- Modular ETL design
- Separation of concerns
- Reusable transformation abstraction
- SQL-to-Pandas logic translation
- Grouping, aggregation, and segmentation logic
- Versioned output handling
- Python (Pandas, NumPy)
- SQL concepts (joins, CASE logic, aggregation)
- GitHub workflow

---
<br>

## 📊 Results

The pipeline:
- Processes raw input files
- Applies business rules
- Produces aggregated outputs ready for reporting

The architecture allows the same transformation logic to be reused with different datasets.

---

<br>

## 👣 Next Steps

Planned improvements:

- Transformation rules passed by parameter in a configuration file.
- Extend Extract class to support API and database integrations (REST, SQL) for dynamic data ingestion
- Workflow orchestration
- Separate visualization module using Matplotlib

> The visualization layer will remain outside the ETL scope to maintain architectural separation.

---

<br>

## 🚀 Deploy and Run the Project

Follow these steps to set up and execute the ETL pipelines locally.

1. Clone the Repository
    git clone https://github.com/jhoalejo07/ETL_Pipelines_Python_1.git <br>
      
2. You may create a new Python environment (Recommended) or use an existing one.
   
3. Install the project dependencies using the requirements.txt file the environment should be activated  <br>
     cd ETL_Pipelines_Python_1  <br>
     pip install -r requirements.txt  <br>
   
4. Open the Project in Your Preferred IDE  <br>
    Open the project folder using your preferred IDE or code editor (for example, VS Code, PyCharm, etc.). <br>
   
5. Execute an ETL Pipeline <br>
    Navigate to the pipeline scripts located in: ./src/etl_pipeline/ <br>
    Run any of the available pipelines: <br>
       Pipeline_BillingHospital.py <br>
       Pipeline_MarketingCampaign.py <br>
       Pipeline_Rentalpy <br>
    Example: <br>
        python ./src/etl_pipeline/Pipeline_BillingHospital.py <br>
   
6. Verify the Output <br>
    After execution, the generated output file will be available at: <br>
    ./data/output/latest.xlsx <br>
    Open the file to verify that the ETL pipeline executed successfully. <br>
---

