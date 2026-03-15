# Modular SQL-Inspired ETL Pipeline (File-Based Data)

<br>

## 📌 Executive Overview

This project implements a **modular ETL pipeline in Pandas Python** to process structured file-based data (CSV, Excel, Parquet).

The goal is to replicate common SQL aggregation and transformation logic in a file-based environment using **Pandas**, while maintaining a clean separation of responsibilities between components.

> ⚠️ This solution is not intended to replace SQL databases.  
> It demonstrates how SQL-style business rules can be applied when working with external data files in a controlled and reusable architecture.
---
<br>

## 🏗️ Architecture

This separation allows reuse of transformation logic across different datasets.

<img width="989" height="682" alt="image" src="https://github.com/user-attachments/assets/a86cbe3f-2b73-4876-9e40-9a39011f1c30" />

<br>

## 💼 Pipelines work flow


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

## 📊 Results

The pipeline:
- Processes raw input files
- Applies business rules
- Produces aggregated outputs ready for reporting

The architecture allows the same transformation logic to be reused with different datasets.

---

## 🚀 Next Steps

Planned improvements:

- Transformation rules passed by parameter in a configuration file.
- Extend Extract class to support API and database integrations (REST, SQL) for dynamic data ingestion
- Workflow orchestration
- Separate visualization module using Matplotlib

> The visualization layer will remain outside the ETL scope to maintain architectural separation.

Separate visualization module using Matplotlib

