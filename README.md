# Data Engineering Project

## Overview
This project implements an ETL (Extract, Transform, Load) pipeline for ingesting, processing, and loading baseball data into a database. The pipeline is designed to handle data from various sources, ensuring high data quality and accessibility for analysis.

Still determining what year this data is from, as Andrew McKirahan is listed here, when I know he hasnt played for the Marlins for a bit - will continue to look at the data and see whats up.

## Project Structure
```
data-engineering-project
├── src
│   ├── ingest
│   │   └── ingest_data.py       # Functions to connect to data sources and ingest data
│   ├── transform
│   │   └── transform_data.py     # Functions to clean and transform the ingested data
│   ├── load
│   │   └── load_data.py          # Functions to load transformed data into a database
│   └── utils
│       └── db_utils.py           # Utility functions for database operations
├── data
│   ├── raw                        # Directory for raw data files
│   └── processed                  # Directory for cleaned and transformed data
├── scripts
│   └── run_etl.py                # Main script to run the ETL process
├── requirements.txt               # Python dependencies for the project
├── README.md                      # Project documentation
└── .gitignore                     # Files and directories to ignore by Git
```

## Getting Started

### Prerequisites
- Python 3.x
- pip (Python package installer)

### Installation
1. Clone the repository:
   ```
   git clone <repository-url>
   cd data-engineering-project
   ```

2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

### Usage
To run the ETL process, execute the following command:
```
python scripts/run_etl.py
```

This will initiate the data ingestion, transformation, and loading processes.

## Findings from Visualizations
- I am not sure what year this data is from.

### Distribution of Player Heights
- The majority of players have heights ranging between 70 and 75 inches.
- There is a noticeable peak around 73 inches, indicating that this is a common height for players.
- I thought it was interesting the height followed a standard bell curve distribution, I wonder if that was intentional in inviting this group to spring training, or if it just so happened to work out that way.

### Distribution of Player Weights
- Player weights are mostly distributed between 180 and 220 pounds.
- The average weight of players is around 200 pounds, with a slight skew towards heavier weights.
-Same distribution of height, although a flatter bell curve so not quite the standard.

### Count of Players by Position Category
- The dataset includes a higher number of pitchers compared to other positions.
- Outfielders and infielders are also well-represented, with a balanced distribution among different positions.
- Obviously this makes sense - pitchers take up the bulk of the roster - I just thought this specific 

### Additional Insights
- The data reveals interesting patterns in player demographics, such as birthplaces and colleges attended.
- There are notable differences in physical attributes (height and weight) across different positions, which could be explored further.

## Contributing
Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for details.