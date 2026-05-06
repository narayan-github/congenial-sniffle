import pandas as pd

# ─────────────────────────────────────────────
# STEP 1: Full Load (Baseline - the bad way)
# ─────────────────────────────────────────────
print("=" * 50)
print("STEP 1: Full Load (Baseline)")
print("=" * 50)
df = pd.read_csv("cdr_day1.csv")
agg = df.groupby("user_id")["call_duration"].sum()
print(agg)
print("❌ Problem: Reloads entire dataset every day\n")


# ─────────────────────────────────────────────
# STEP 2: Watermarking (Incremental Ingestion)
# ─────────────────────────────────────────────
print("=" * 50)
print("STEP 2: Watermarking")
print("=" * 50)
last_processed_time = "2024-01-01"
df_new = pd.read_csv("cdr_day2.csv")
df_incremental = df_new[df_new["last_updated"] > last_processed_time]
print(df_incremental)
print("✅ Benefit: Only new/updated records processed\n")


# ─────────────────────────────────────────────
# STEP 3: CDC - Change Data Capture Simulation
# ─────────────────────────────────────────────
print("=" * 50)
print("STEP 3: CDC Simulation")
print("=" * 50)
df_old = pd.read_csv("cdr_day1.csv")
df_new = pd.read_csv("cdr_day2.csv")

df_combined = pd.concat([df_old, df_new])
df_combined = df_combined.sort_values("last_updated").drop_duplicates("call_id", keep="last")
print(df_combined)
print("✅ Updated records replace old ones (CDC behavior)\n")


# ─────────────────────────────────────────────
# STEP 4: Aggregation
# ─────────────────────────────────────────────
print("=" * 50)
print("STEP 4: Aggregation after Incremental Load")
print("=" * 50)
agg = df_combined.groupby("user_id")["call_duration"].sum().reset_index()
print(agg)
print()


# ─────────────────────────────────────────────
# STEP 5: Persist Watermark
# ─────────────────────────────────────────────
print("=" * 50)
print("STEP 5: Saving Watermark")
print("=" * 50)
last_processed_time = df_combined["last_updated"].max()
with open("watermark.txt", "w") as f:
    f.write(last_processed_time)
print(f"✅ Watermark saved: {last_processed_time}\n")


# ─────────────────────────────────────────────
# STEP 6: Full Pipeline Function
# ─────────────────────────────────────────────
print("=" * 50)
print("STEP 6: Full Pipeline")
print("=" * 50)

def load_data(file):
    return pd.read_csv(file)

def incremental_load(df, last_time):
    return df[df["last_updated"] > last_time]

def apply_cdc(old_df, new_df):
    df = pd.concat([old_df, new_df])
    df = df.sort_values("last_updated").drop_duplicates("call_id", keep="last")
    return df

def aggregate(df):
    return df.groupby("user_id")["call_duration"].sum().reset_index()

def run_pipeline():
    old_df = load_data("cdr_day1.csv")
    new_df = load_data("cdr_day2.csv")
    inc_df = incremental_load(new_df, "2024-01-01")
    final_df = apply_cdc(old_df, inc_df)
    result = aggregate(final_df)
    print(result)

run_pipeline()
print()


# ─────────────────────────────────────────────
# EXERCISE 1: Add network_type column (4G/5G)
# ─────────────────────────────────────────────
print("=" * 50)
print("EXERCISE 1: Add network_type Column")
print("=" * 50)

df_old_ex1 = pd.read_csv("cdr_day1.csv")
df_new_ex1 = pd.read_csv("cdr_day2.csv")

# Simulate network_type assignment
df_old_ex1["network_type"] = ["4G", "5G", "4G"]
df_new_ex1["network_type"] = ["5G", "5G", "4G"]

df_combined_ex1 = pd.concat([df_old_ex1, df_new_ex1])
df_combined_ex1 = df_combined_ex1.sort_values("last_updated").drop_duplicates("call_id", keep="last")

agg_ex1 = df_combined_ex1.groupby(["user_id", "network_type"])["call_duration"].sum().reset_index()
print(agg_ex1)
print()


# ─────────────────────────────────────────────
# EXERCISE 2: Late Arriving Data
# ─────────────────────────────────────────────
print("=" * 50)
print("EXERCISE 2: Late Arriving Data")
print("=" * 50)

df_old_ex2 = pd.read_csv("cdr_day1.csv")
df_late = pd.read_csv("cdr_day2_late.csv")

# Watermark still works — late record has new last_updated
last_time = "2024-01-01"
df_inc_ex2 = df_late[df_late["last_updated"] > last_time]
print("Incremental (including late record):")
print(df_inc_ex2)

df_final_ex2 = pd.concat([df_old_ex2, df_inc_ex2])
df_final_ex2 = df_final_ex2.sort_values("last_updated").drop_duplicates("call_id", keep="last")
print("\nAggregation with late record:")
print(df_final_ex2.groupby("user_id")["call_duration"].sum().reset_index())
print()


# ─────────────────────────────────────────────
# EXERCISE 3: Delete Simulation
# ─────────────────────────────────────────────
print("=" * 50)
print("EXERCISE 3: Delete Simulation (is_deleted flag)")
print("=" * 50)

df_old_ex3 = pd.read_csv("cdr_day1.csv")
df_old_ex3["is_deleted"] = False

df_delete = pd.read_csv("cdr_day2_delete.csv")

df_combined_ex3 = pd.concat([df_old_ex3, df_delete])
df_combined_ex3 = df_combined_ex3.sort_values("last_updated").drop_duplicates("call_id", keep="last")

# Remove soft-deleted records
df_active = df_combined_ex3[df_combined_ex3["is_deleted"] == False]

print("Active records after delete simulation:")
print(df_active)
print("\nFinal Aggregation (deleted records excluded):")
print(df_active.groupby("user_id")["call_duration"].sum().reset_index())
