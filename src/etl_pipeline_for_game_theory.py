import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Initialize SparkSession
spark = SparkSession.builder \
    .appName("LoLJungleMatrixAnalysis") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# 2. Define relative paths dynamically (Secure against path leakage)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../data"))

data_path = os.path.join(DATA_DIR, "*.csv")
output_path = os.path.join(DATA_DIR, "processed_jungle_matrix")

# 3. Read raw CSV dataset
df = spark.read.option("header", "true").option("inferSchema", "true").csv(data_path)
print(f"Total rows in raw dataset: {df.count()}")

# 4. Define game duration in minutes
game_minutes = F.col("gameDuration") / 60.0

# NOTE: Based on standard Riot 10-player structure where:
# participant1 is typically Blue Team Jungle (Team 100)
# participant6 is typically Red Team Jungle (Team 200)
blue_jungle_prefix = "participant1"
red_jungle_prefix = "participant6"

# 5. Define key jungle performance metrics for the 3x3 matrix
jungle_metrics = [
    "GoldEarned",
    "ChampExperience",
    "TotalDamageDealtToChampions",
    "DamageDealtToObjectives",
    "NeutralMinionsKilled",
    "TotalEnemyJungleMinionsKilled", # Key metric for counter-jungling/invasion
    "VisionScore"
]

# Initialize select expression list
select_exprs = [
    "matchId",
    "gameDuration",
    "team0Win"
]

# 6. Dynamically calculate per-minute difference between Blue Jungle and Red Jungle
for metric in jungle_metrics:
    blue_val = F.col(f"{blue_jungle_prefix}{metric}")
    red_val = F.col(f"{red_jungle_prefix}{metric}")
    
    # Compute per-minute rate difference
    diff_expr = ((blue_val - red_val) / game_minutes).alias(f"jungle{metric}PerMinDiff")
    select_exprs.append(diff_expr)

# 7. Add objective secure differences (e.g., Baron/Dragon assists or steals)
select_exprs.extend([
    (F.col(f"{blue_jungle_prefix}ObjectivesStolen") - F.col(f"{red_jungle_prefix}ObjectivesStolen")).alias("jungleObjectivesStolenDiff")
])

# Execute projection
processed_df = df.select(select_exprs)

# 8. Preview extracted features
print("=== Jungle 3x3 Matrix Features Preview ===")
processed_df.show(5, truncate=False)

# 9. Export to CSV using coalesce(1) for clean output
processed_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
print(f"Jungle matrix features successfully exported to: {output_path}")