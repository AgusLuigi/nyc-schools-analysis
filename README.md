# nyc-schools-analysis
An end-to-end data engineering and analysis pipeline for NYC School data, featuring SQL integration, automated ETL, and interactive Python notebooks.

# 🍎 NYC Schools Data Analysis Portfolio

## 🎯 Motivation
This project was developed during the **Data Analytics Onboarding 2026** to demonstrate a full-cycle data workflow. The goal was to transform raw NYC Open Data into actionable insights regarding school safety and academic performance.

## ⚙️ The Workflow (ETL)
1.  **Exploration:** Initial analysis of NYC School Directories and Safety Reports using `Pandas`.
2.  **Processing:** Data cleaning, handling missing values, and type conversion.
3.  **Database Integration:** Building an automated pipeline to upload cleaned data into a SQL database using `SQLAlchemy`.
4.  **Analysis:** Executing complex SQL queries via Python to correlate school size with incident rates.

## 📊 Key Results
- Identified specific boroughs with disproportionate safety incident counts.
- Created a structured SQL environment for longitudinal school data tracking.
- Automated the reporting structure, moving from daily task logs to a thematic project architecture.

## 🛠️ Tech Stack
- **Language:** Python 3.10+
- **Data:** Pandas, NumPy
- **Database:** SQLite / SQLAlchemy (ORM)
- **Viz:** Matplotlib, Seaborn
