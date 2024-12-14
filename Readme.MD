<h2 align="center">✨ Crash Analysis - Data Engineering Case Study ✨</h2>


## 📝 Table of Contents
- [Introduction](#-introduction)
- [Prerequisites](#️-prerequisites)
- [Python Dependencies](#-python-dependencies)
- [Project Structure](#️-project-structure)
- [Running the Application](#-running-the-application)
  - [Running All Analyses](#️-running-all-analyses)
  - [Running a Specific Analysis](#-running-a-specific-analysis)


## 📘 Introduction
This project performs crash data analytics using PySpark. It provides 10 specific analyses to generate insights about crashes based on the given datasets.
- Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
- Analysis 2: How many two wheelers are booked for crashes? 
- Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
- Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? 
- Analysis 5: Which state has highest number of accidents in which females are not involved? 
- Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
- Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
- Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
- Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
- Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

### 🛠️ Prerequisites
- Python 3.10+
- Apache Spark 3.5.3
- Java 17

### 📦 Python Dependencies
- pyspark==3.5.3
- PyYAML==6.0.2
- pytest==8.3.4

## ✍️ Project Structure
```
.
├── config.yaml                   # Configuration file for input/output paths
├── data/
│   ├── input/                    # Input datasets
│   │   ├── Charges_use.csv
│   │   ├── Endorse_use.csv
│   │   ├── Restrict_use.csv
│   │   ├── Damages_use.csv
│   │   ├── Primary_Person_use.csv
│   │   └── Units_use.csv
│   └── output/                   # Output directory for analysis results
└── utils/
    ├── utils.py                  # Utility functions (e.g., reading config files)
    ├── analysis.py               # Contains all 10 analysis methods
    ├── logger.py                 # Logger object for logging
    └── __init__.py
├── main.py                       # Main script to run analyses
├── requirements.txt              # Python dependencies
├── notebook.ipynb                # A notebook where all the analyses are performed
├── README.md
├── .gitignore
└── tests/
    ├── test_utils.py             # Test cases for utility functions
    └── __init__.py
```

## 🚀 Running the Application

### 📥 Install All Required Prerequisites

### 📥 Install All Python Dependencies
```bash
pip install -r requirements.txt
```

### ▶️ Running All Analyses
To run all 10 analyses, use the following command:
```bash
spark-submit --master "local[*]" main.py
```
This will sequentially execute all analyses and save their results in the `data/output/` directory.

### 🎯 Running a Specific Analysis
To run a specific analysis, pass the `--analysis` argument with the desired analysis number (1-10):
```bash
spark-submit --master "local[*]" main.py --analysis 3
```
This will execute only the 3rd analysis.

A version of all analyses is available in the notebook.ipynb
