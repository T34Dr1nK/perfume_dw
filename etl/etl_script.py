import os
import zipfile
import pandas as pd
from sqlalchemy import create_engine, text

# =========================
# 📥 EXTRACT: DOWNLOAD & UNZIP DATASET
# =========================

# Set Kaggle credentials
os.environ["KAGGLE_CONFIG_DIR"] = "/app"

# Download dataset from Kaggle
os.system("kaggle datasets download -d olgagmiufana1/fragrantica-com-fragrance-dataset -p /app")

# Extract ZIP file
zip_path = "/app/fragrantica-com-fragrance-dataset.zip"
with zipfile.ZipFile(zip_path, "r") as zip_ref:
    zip_ref.extractall("/app")

# Locate CSV file
csv_path = "/app/fra_cleaned.csv"
if not os.path.exists(csv_path):
    raise FileNotFoundError("❌ `fra_cleaned.csv` not found after extraction!")

print(f"📂 Using dataset: {csv_path}")

# =========================
# 🧹 TRANSFORM: DATA CLEANING & NORMALIZATION
# =========================

# Load dataset
df = pd.read_csv(csv_path, delimiter=';', encoding="latin1", on_bad_lines='skip')

# Standardize column names
df.columns = df.columns.str.strip()

print("🔍 Columns in dataset:", df.columns.tolist())

# Convert numeric columns safely
df["Rating Count"] = pd.to_numeric(df["Rating Count"].astype(str).str.replace(',', '.', regex=True), errors="coerce")
df["Rating Value"] = pd.to_numeric(df["Rating Value"].astype(str).str.replace(',', '.', regex=True), errors="coerce")
df["Year"] = df["Year"].astype(str).replace("nan", "Not Specified")

# Handle missing values
df.fillna({"Perfumer1": "Unknown", "Perfumer2": "Unknown",
           "mainaccord1": "Unknown", "mainaccord2": "Unknown",
           "mainaccord3": "Unknown", "mainaccord4": "Unknown",
           "mainaccord5": "Unknown", "Top": "Unknown",
           "Middle": "Unknown", "Base": "Unknown"}, inplace=True)

# Remove duplicates & drop critical missing data
df.drop_duplicates(inplace=True)
df.dropna(subset=["Perfume", "Brand", "Rating Value"], inplace=True)

# =========================
# 🏛️ LOAD: DATA WAREHOUSE SCHEMA
# =========================

# Connect to PostgreSQL
DB_URL = 'postgresql://dw_user:password@postgres:5432/perfume_dw'
engine = create_engine(DB_URL)

with engine.connect() as conn:
    # Brands Table
    brands = df[["Brand"]].drop_duplicates().reset_index(drop=True).rename(columns={"Brand": "brand_name"})
    brands["brand_id"] = brands.index + 1
    brands.to_sql("brands", conn, if_exists="replace", index=False)

    # Countries Table
    countries = df[["Country"]].drop_duplicates().reset_index(drop=True).rename(columns={"Country": "country_name"})
    countries["country_id"] = countries.index + 1
    countries.to_sql("countries", conn, if_exists="replace", index=False)

    # Genders Table
    genders = df[["Gender"]].drop_duplicates().reset_index(drop=True).rename(columns={"Gender": "gender_name"})
    genders["gender_id"] = genders.index + 1
    genders.to_sql("genders", conn, if_exists="replace", index=False)

    # Perfumes Table
    perfumes = df[["Perfume", "Brand", "Country", "Gender", "Rating Value", "Rating Count", "Year", "url"]]
    perfumes = perfumes.merge(brands, left_on="Brand", right_on="brand_name").merge(
        countries, left_on="Country", right_on="country_name").merge(
        genders, left_on="Gender", right_on="gender_name")

    perfumes = perfumes.rename(columns={"Perfume": "name", "Rating Value": "rating_value", "Rating Count": "rating_count"})
    perfumes = perfumes[["name", "brand_id", "country_id", "gender_id", "rating_value", "rating_count", "Year", "url"]]
    perfumes["perfume_id"] = perfumes.index + 1  # Primary Key
    perfumes.to_sql("perfumes", conn, if_exists="replace", index=False)

    # Accords Table
    accords_df = pd.melt(df, id_vars=["Perfume"], value_vars=["mainaccord1", "mainaccord2", "mainaccord3", "mainaccord4", "mainaccord5"],
                          var_name="accord_rank", value_name="accord_name").dropna().drop_duplicates()
    accords = accords_df[["accord_name"]].drop_duplicates().reset_index(drop=True)
    accords["accord_id"] = accords.index + 1
    accords.to_sql("accords", conn, if_exists="replace", index=False)

    # Perfume-Accords Relationship
    perfume_accords = accords_df.merge(accords, on="accord_name").merge(perfumes, left_on="Perfume", right_on="name")
    perfume_accords = perfume_accords[["perfume_id", "accord_id", "accord_rank"]]
    perfume_accords.to_sql("perfume_accords", conn, if_exists="replace", index=False)

    # Notes Table
    notes_df = pd.melt(df, id_vars=["Perfume"], value_vars=["Top", "Middle", "Base"], var_name="note_type", value_name="note_name").dropna().drop_duplicates()
    notes = notes_df[["note_name"]].drop_duplicates().reset_index(drop=True)
    notes["note_id"] = notes.index + 1
    notes.to_sql("notes", conn, if_exists="replace", index=False)

    # Perfume-Notes Relationship
    perfume_notes = notes_df.merge(notes, on="note_name").merge(perfumes, left_on="Perfume", right_on="name")
    perfume_notes = perfume_notes[["perfume_id", "note_id", "note_type"]]
    perfume_notes.to_sql("perfume_notes", conn, if_exists="replace", index=False)

    # Perfumers Table
    perfumers_df = pd.melt(df, id_vars=["Perfume"], value_vars=["Perfumer1", "Perfumer2"], var_name="perfumer_type", value_name="perfumer_name").dropna().drop_duplicates()
    perfumers = perfumers_df[["perfumer_name"]].drop_duplicates().reset_index(drop=True)
    perfumers["perfumer_id"] = perfumers.index + 1
    perfumers.to_sql("perfumers", conn, if_exists="replace", index=False)

    # Perfume-Perfumers Relationship
    perfume_perfumers = perfumers_df.merge(perfumers, on="perfumer_name").merge(perfumes, left_on="Perfume", right_on="name")
    perfume_perfumers = perfume_perfumers[["perfume_id", "perfumer_id"]]
    perfume_perfumers.to_sql("perfume_perfumers", conn, if_exists="replace", index=False)

    # =========================
    # 🏎️ PERFORMANCE OPTIMIZATION
    # =========================
    try:
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_brand_id ON perfumes ("brand_id");'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_country_id ON perfumes ("country_id");'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_gender_id ON perfumes ("gender_id");'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_perfume_id_notes ON perfume_notes ("perfume_id");'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_perfume_id_accords ON perfume_accords ("perfume_id");'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_perfume_id_perfumers ON perfume_perfumers ("perfume_id");'))
        print("✅ Indexes created successfully!")
    except Exception as e:
        print(f"⚠️ Indexing failed: {e}")

    print("🚀 ETL Process Completed Successfully!")
