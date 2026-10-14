lol-bigdata-project/
├── data/                  # 存放原始資料與中間資料
│   └── match_timeline.csv # 從本機拖曳上傳的 700MB+ 原始資料
│
├── src/                   # 存放所有的 Python / PySpark 程式碼
│   ├── __init__.py        # 讓 src 成為 Python 套件的空檔案
│   ├── etl_pipeline_for_training.py    # PySpark ETL 資料清洗與特徵工程
│   ├──etl_pipeline_for_game_theory.py  # PySpark ETL 資料清洗與特徵工程
│   ├── payoff_matrix.py   # 建構 3x3 打野博弈矩陣邏輯
│   └── scaling_test.py    # 執行 1, 2, 4 核心強擴展性基準測試
│
├── README.md              # 專案說明文件（記錄架構與執行方法）
└── requirements.txt       # Python 依賴套件清單 (如 pyspark 等)