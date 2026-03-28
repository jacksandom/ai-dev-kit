# Data I/O: Reading and Writing Data

Reference for converting R data I/O operations to PySpark on Databricks.

## Reading Data

### CSV Files

**R:**
```r
df <- read.csv("data/sales.csv", stringsAsFactors = FALSE)
df <- readr::read_csv("data/sales.csv", col_types = cols(id = col_integer(), name = col_character()))
df <- data.table::fread("data/sales.csv")
```

**PySpark:**
```python
# From Unity Catalog Volume (recommended)
df = spark.read.csv("/Volumes/catalog/schema/volume/sales.csv", header=True, inferSchema=True)

# With explicit schema (faster, more reliable than inferSchema)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("amount", DoubleType(), True),
])
df = spark.read.csv("/Volumes/catalog/schema/volume/sales.csv", header=True, schema=schema)
```

**Common CSV options:**

| R parameter | PySpark parameter |
|-------------|-------------------|
| `sep = "\t"` | `sep="\t"` |
| `header = TRUE` | `header=True` |
| `na.strings = c("", "NA", "NULL")` | `nullValue="NA"` (single value only; use `.replace()` for multiple) |
| `skip = 1` | Not direct — use `.filter()` after read |
| `nrows = 100` | Not direct — use `.limit(100)` after read |
| `encoding = "UTF-8"` | `encoding="UTF-8"` |
| `quote = '"'` | `quote='"'` |

### Parquet Files

**R:**
```r
df <- arrow::read_parquet("data/output.parquet")
```

**PySpark:**
```python
df = spark.read.parquet("/Volumes/catalog/schema/volume/output.parquet")
```

### Excel Files

**R:**
```r
df <- readxl::read_excel("data/report.xlsx", sheet = "Sheet1")
```

**PySpark:**
```python
# Option 1: Use pandas as intermediary (small files only)
import pandas as pd
pdf = pd.read_excel("/Volumes/catalog/schema/volume/report.xlsx", sheet_name="Sheet1")
df = spark.createDataFrame(pdf)

# Option 2: Use com.crealytics.spark.excel (requires library installation)
df = (spark.read.format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("dataAddress", "'Sheet1'!A1")
    .load("/Volumes/catalog/schema/volume/report.xlsx"))
```

### JSON Files

**R:**
```r
df <- jsonlite::fromJSON("data/events.json")
```

**PySpark:**
```python
df = spark.read.json("/Volumes/catalog/schema/volume/events.json")

# Multiline JSON
df = spark.read.json("/Volumes/catalog/schema/volume/events.json", multiLine=True)
```

### Database Tables (DBI / RODBC / RJDBC)

**R:**
```r
library(DBI)
con <- dbConnect(odbc::odbc(), dsn = "my_database")
df <- dbReadTable(con, "schema.table_name")
df <- dbGetQuery(con, "SELECT * FROM schema.table_name WHERE year = 2024")
```

**PySpark:**
```python
# Unity Catalog table (the primary pattern on Databricks)
df = spark.table("catalog.schema.table_name")

# SQL query
df = spark.sql("SELECT * FROM catalog.schema.table_name WHERE year = 2024")
```

### RDS / RData (R-specific formats)

**R:**
```r
df <- readRDS("data/model_data.rds")
load("data/workspace.RData")
```

**PySpark:** No direct equivalent. These are R-specific binary formats.

**Migration strategy:**
1. In R, export to Parquet or CSV: `arrow::write_parquet(df, "data/model_data.parquet")`
2. Upload to a Unity Catalog Volume
3. Read in PySpark: `df = spark.read.parquet("/Volumes/catalog/schema/volume/model_data.parquet")`

---

## Writing Data

### CSV

**R:**
```r
write.csv(df, "output/results.csv", row.names = FALSE)
readr::write_csv(df, "output/results.csv")
```

**PySpark:**
```python
# Write to Volume
df.write.mode("overwrite").option("header", True).csv("/Volumes/catalog/schema/volume/results")

# Single file (small data only)
df.coalesce(1).write.mode("overwrite").option("header", True).csv("/Volumes/catalog/schema/volume/results")
```

### Delta Table (recommended on Databricks)

**R:**
```r
# No direct R equivalent — this is the preferred PySpark output format
```

**PySpark:**
```python
# Write as managed Delta table in Unity Catalog
df.write.mode("overwrite").saveAsTable("catalog.schema.results")

# Append to existing table
df.write.mode("append").saveAsTable("catalog.schema.results")

# Merge / upsert (replaces R logic with conditional insert/update)
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "catalog.schema.results")
target.alias("t").merge(
    df.alias("s"),
    "t.id = s.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

### Parquet

**R:**
```r
arrow::write_parquet(df, "output/results.parquet")
```

**PySpark:**
```python
df.write.mode("overwrite").parquet("/Volumes/catalog/schema/volume/results.parquet")
```

---

## Parameterization

### Replacing R command-line arguments

**R:**
```r
args <- commandArgs(trailingOnly = TRUE)
input_date <- args[1]
region <- args[2]

# Or using config files
config <- yaml::read_yaml("config.yml")
```

**PySpark (Databricks widgets):**
```python
# Define widgets with defaults
dbutils.widgets.text("input_date", "2024-01-01", "Input Date")
dbutils.widgets.dropdown("region", "US", ["US", "UK", "DE", "FR"], "Region")

# Read widget values
input_date = dbutils.widgets.get("input_date")
region = dbutils.widgets.get("region")

# Use in queries
df = spark.table("catalog.schema.orders").filter(
    (F.col("order_date") >= input_date) & (F.col("region") == region)
)
```

> **Benefit:** Widgets create UI elements in Databricks notebooks and can be passed as job parameters.

### Replacing environment variables

**R:**
```r
api_key <- Sys.getenv("API_KEY")
db_host <- Sys.getenv("DB_HOST")
```

**PySpark:**
```python
# Use Databricks secrets (more secure than env vars)
api_key = dbutils.secrets.get(scope="my-scope", key="api-key")
```

---

## Auto Loader (streaming file ingestion)

For production pipelines that continuously ingest new files, replace R's file-watching scripts with Auto Loader:

**R (manual polling):**
```r
while (TRUE) {
  new_files <- list.files("data/incoming/", pattern = "*.csv", full.names = TRUE)
  for (f in new_files) {
    df <- read.csv(f)
    # process...
    file.rename(f, paste0("data/processed/", basename(f)))
  }
  Sys.sleep(60)
}
```

**PySpark (Auto Loader):**
```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/catalog/schema/volume/_schemas/incoming")
    .load("/Volumes/catalog/schema/volume/incoming/")
)

(
    df.writeStream
    .option("checkpointLocation", "/Volumes/catalog/schema/volume/_checkpoints/incoming")
    .trigger(availableNow=True)
    .toTable("catalog.schema.incoming_data")
)
```

---

## Quick Reference: I/O Function Mapping

| R Function | PySpark Equivalent | Notes |
|------------|-------------------|-------|
| `read.csv()` / `readr::read_csv()` | `spark.read.csv()` | Use `header=True, inferSchema=True` |
| `data.table::fread()` | `spark.read.csv()` with explicit schema | Schema is faster than `inferSchema` |
| `readRDS()` | N/A | Export to Parquet first, then read |
| `load()` (.RData) | N/A | Export individual objects to Parquet |
| `arrow::read_parquet()` | `spark.read.parquet()` | |
| `readxl::read_excel()` | `pd.read_excel()` → `spark.createDataFrame()` | Small files only |
| `jsonlite::fromJSON()` | `spark.read.json()` | |
| `DBI::dbReadTable()` | `spark.table("catalog.schema.table")` | |
| `DBI::dbGetQuery()` | `spark.sql("SELECT ...")` | |
| `write.csv()` | `df.write.csv()` | Prefer Delta: `.saveAsTable()` |
| `arrow::write_parquet()` | `df.write.parquet()` | |
| `saveRDS()` | `df.write.saveAsTable()` | Use Delta format |
| `commandArgs()` | `dbutils.widgets.get()` | |
| `Sys.getenv()` | `dbutils.secrets.get()` | |
