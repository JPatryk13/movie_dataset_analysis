# movie_analysis

The project concerns data engineering, including ETL processes and data visualization in the Tableau tool. This repository contains all the necessary files and documentation needed to run the project.

## Requirements
Before starting your project, make sure you have the following tools installed:

- Python (3.8 or later recommended)
- PostgreSQL
- Tableau Desktop (if you want to visualize)
- Additionally, it is recommended to use a Python virtual environment such as venv.

## Installation
Clone the repository to the selected directory or open zip file

Navigate to the project directory:
``` 
cd movie_dataset_analysis
```

Create and activate a virtual environment:
```
python -m venv venv
source venv/bin/activate  # on Linux/Mac
venv\Scripts\activate     # on Windows
```

Install the required packages from the requirements.txt file:
``` 
pip install -r requirements.txt
```

## Download datasets
From movies dataset on webpage: https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset/

Download to "dataset" directory files:
- credits.csv
- keywords.csv
- movies_metadata.csv
- ratings.csv


## Execution
1. Configure your database connection by editing the config.py
2. Execute the ETL Cleaning and Transforming processes
```
python save_cleaned_df_app.py
```
3. Execute Loading to DB processes
```
python loading_app.py
```
