lol-bigdata-project/
├── data/                                 # Raw and intermediate data directory (Git ignored)
│   └── match_timeline.csv                # Raw 700MB+ match timeline dataset
├── src/                                  # Source directory for Python / PySpark scripts
│   ├── __init__.py                       # Marks src as a Python package
│   ├── etl_pipeline_for_training.py      # PySpark ETL pipeline for win rate prediction feature engineering
│   ├── etl_pipeline_for_game_theory.py   # PySpark ETL pipeline for 3x3 jungle game theory analysis
│   ├── payoff_matrix.py                  # 3x3 jungle payoff matrix construction and game theory logic
│   └── scaling_test.py                   # Strong scalability benchmark test (1, 2, 4 cores)
├── README.md                             # Project documentation
└── requirements.txt                      # Python dependencies list (PySpark, etc.)