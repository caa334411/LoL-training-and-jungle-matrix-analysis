import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Initialize SparkSession
spark = SparkSession.builder \
    .appName("ProcessLoLAllPerMinuteDataset") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# 2. Define relative paths dynamically
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

data_path = os.path.join(DATA_DIR, "*.csv")
output_path = os.path.join(DATA_DIR, "processed_per_minute_features")

# 3. Read raw CSV files
df = spark.read.option("header", "true").option("inferSchema", "true").csv(data_path)
print(f"Total rows in raw dataset: {df.count()}")

# 4. Calculate game minutes
game_minutes = F.col("gameDuration") / 60.0

# 5. Define metrics to process via dynamic loops
metrics = [
    "GoldEarned", 
    "TotalDamageDealtToChampions", 
    "ChampExperience", 
    "TotalMinionsKilled", 
    "NeutralMinionsKilled", 
    "VisionScore"
]

select_exprs = [
    "matchId",
    "gameDuration",
    "team0Win"
]

# 6. Dynamically sum participant data and compute per-minute differences
for metric in metrics:
    team100_sum = sum(F.col(f"participant{i}{metric}") for i in range(5))
    team200_sum = sum(F.col(f"participant{i}{metric}") for i in range(5, 10))
    
    diff_expr = ((team100_sum - team200_sum) / game_minutes).alias(f"team{metric}PerMinDiff")
    select_exprs.append(diff_expr)

# 7. Add team-level metrics
select_exprs.extend([
    ((F.col("team0ChampionKills") - F.col("team1ChampionKills")) / game_minutes).alias("teamKillsPerMinDiff"),
    (F.col("team0DragonKills") - F.col("team1DragonKills")).alias("teamDragonDiff"),
    (F.col("team0BaronKills") - F.col("team1BaronKills")).alias("teamBaronDiff"),
    (F.col("team0TowerKills") - F.col("team1TowerKills")).alias("teamTowerDiff")
])

processed_df = df.select(select_exprs)

# 8. Preview features
print("=== Feature Preview ===")
processed_df.show(5, truncate=False)

# 9. Export processed data
processed_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
print(f"Features exported successfully to: {output_path}")