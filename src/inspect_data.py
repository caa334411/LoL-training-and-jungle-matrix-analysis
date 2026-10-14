from pyspark.sql import SparkSession

# 初始化 Spark 程式
spark = SparkSession.builder \
    .appName("InspectLoLData") \
    .master("local[*]") \
    .getOrCreate()

# 讀取 CSV 檔案（請將檔名替換為你實際上傳的檔案名稱）
file_path = "/mnt/blockstorage/lol-bigdata-project/data/matchData.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

# 1. 取得第一筆資料 (Row 物件)
first_row = df.first()

# 2. 迴圈印出所有欄位名稱及其數值
print("=== 第一筆資料的所有變數與數值 ===")
for col_name in df.columns:
    print(f"{col_name}: {first_row[col_name]}")