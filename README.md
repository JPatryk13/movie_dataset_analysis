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
1. Configure your database connection by editing the ```config.py``` file
2. Execute the ETL Cleaning and Transforming processes
```
python save_cleaned_df_app.py
```
3. Execute Loading to DB processes
```
python loading_app.py
```
4. After the ETL operations, you can export the data to a Tableau-friendly format or use the provided scripts/tools within the project for visualization in Tableau.


## Support
If you encounter any issues or have questions regarding the project's operation, please use the "Issues" section on GitHub or contact directly via email.

## License
This project is available under the MIT license. More details can be found in the LICENSE file.


