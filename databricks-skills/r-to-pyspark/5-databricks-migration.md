# Databricks Migration Guide: R Scripts to PySpark

End-to-end guide for migrating R workloads to PySpark on Databricks.

## Migration Workflow

### Step 1: Assess the R Codebase

Before converting, inventory what the R code does:

| Assessment Area | What to Look For | Migration Impact |
|----------------|------------------|-----------------|
| **Data sources** | `read.csv`, `DBI::dbConnect`, `readRDS` | Map to `spark.read`, `spark.table`, Volumes |
| **Libraries used** | `dplyr`, `data.table`, `ggplot2`, `caret` | Map to PySpark, matplotlib, sklearn |
| **Output targets** | `write.csv`, `saveRDS`, database writes | Map to Delta tables, Volumes |
| **Parameters** | `commandArgs()`, config files, env vars | Map to `dbutils.widgets`, secrets |
| **External APIs** | `httr`, `curl`, API calls | Use Python `requests` library |
| **Statistical models** | `lm`, `glm`, `randomForest` | Use `sklearn`, `mlflow`, `pyspark.ml` |
| **Visualizations** | `ggplot2`, `plotly`, `shiny` | Use matplotlib/plotly + `display()`, Databricks Apps |
| **Parallel processing** | `parallel`, `foreach`, `future` | Spark handles parallelism natively |

### Step 2: Set Up Databricks Environment

```python
# Create catalog and schema for the migrated workload
spark.sql("CREATE CATALOG IF NOT EXISTS my_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS my_catalog.my_schema")

# Create a volume for file-based data
spark.sql("CREATE VOLUME IF NOT EXISTS my_catalog.my_schema.data")
```

Upload any CSV/Parquet files the R scripts read to the volume:
```
/Volumes/my_catalog/my_schema/data/input_file.csv
```

### Step 3: Convert Incrementally

Convert one script at a time. For each script:

1. Create a new Databricks notebook
2. Convert the R code using this skill's reference files
3. Test with a small data sample first
4. Compare output counts and values against R results
5. Move to the next script

### Step 4: Validate and Schedule

After conversion, set up as a Databricks Job for production scheduling.

---

## R Package Equivalents

| R Package | Python/PySpark Equivalent | Notes |
|-----------|--------------------------|-------|
| **dplyr** | PySpark DataFrame API | See [1-data-manipulation.md](1-data-manipulation.md) |
| **tidyr** | PySpark pivot/unpivot/stack | See [4-advanced-patterns.md](4-advanced-patterns.md) |
| **data.table** | PySpark DataFrame API | `fread` → `spark.read.csv` with schema |
| **ggplot2** | `matplotlib` + `display()` | See visualization section below |
| **plotly** | `plotly` + `display()` | Works directly in Databricks |
| **stringr** | `pyspark.sql.functions` string ops | See [3-functions-and-types.md](3-functions-and-types.md) |
| **lubridate** | `pyspark.sql.functions` date ops | See [3-functions-and-types.md](3-functions-and-types.md) |
| **readr** | `spark.read.csv` / `spark.read.json` | See [2-data-io.md](2-data-io.md) |
| **readxl** | `pandas.read_excel` → `spark.createDataFrame` | Small files only |
| **DBI / RODBC** | `spark.table()` / `spark.sql()` | Unity Catalog replaces ODBC |
| **caret** | `sklearn` + `mlflow` | Use MLflow for experiment tracking |
| **tidymodels** | `sklearn` + `mlflow` | |
| **randomForest** | `sklearn.ensemble.RandomForestClassifier` or `pyspark.ml` | |
| **xgboost** | `xgboost` (Python) | Same library, Python API |
| **glmnet** | `sklearn.linear_model` | |
| **shiny** | Databricks Apps (Streamlit/Dash/Gradio) | See `databricks-app-python` skill |
| **rmarkdown** | Databricks Notebooks | Native markdown cells |
| **parallel / foreach** | Spark native parallelism | No explicit parallelism needed |
| **httr / curl** | `requests` library | |
| **jsonlite** | `json` library / `spark.read.json` | |
| **yaml** | `pyyaml` library | |
| **testthat** | `pytest` | |
| **arrow** | `spark.read.parquet` | Arrow is Spark's internal format |
| **dbplyr** | `spark.sql()` / `spark.table()` | Direct SQL on Spark |

---

## Visualization Conversion

### ggplot2 → matplotlib + display()

**R:**
```r
library(ggplot2)

ggplot(df, aes(x = date, y = revenue, color = region)) +
  geom_line() +
  geom_point() +
  labs(title = "Revenue by Region", x = "Date", y = "Revenue ($)") +
  theme_minimal()
```

**PySpark notebook:**
```python
import matplotlib.pyplot as plt

# Convert to pandas for plotting (small aggregated data only)
pdf = result.toPandas()

fig, ax = plt.subplots(figsize=(12, 6))
for region in pdf["region"].unique():
    region_data = pdf[pdf["region"] == region]
    ax.plot(region_data["date"], region_data["revenue"], marker="o", label=region)

ax.set_title("Revenue by Region")
ax.set_xlabel("Date")
ax.set_ylabel("Revenue ($)")
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()
display(fig)
```

### ggplot2 geom mapping

| ggplot2 Geom | matplotlib Equivalent |
|-------------|----------------------|
| `geom_point()` | `ax.scatter(x, y)` |
| `geom_line()` | `ax.plot(x, y)` |
| `geom_bar()` | `ax.bar(x, y)` |
| `geom_histogram()` | `ax.hist(x, bins=30)` |
| `geom_boxplot()` | `ax.boxplot(data)` |
| `geom_hline(yintercept=v)` | `ax.axhline(y=v)` |
| `geom_vline(xintercept=v)` | `ax.axvline(x=v)` |
| `geom_text(label=x)` | `ax.annotate(text, (x, y))` |
| `geom_area()` | `ax.fill_between(x, y)` |
| `facet_wrap(~var)` | `fig, axes = plt.subplots(nrows, ncols)` |

### Alternative: Databricks native visualization

For simple charts, skip matplotlib entirely and use Databricks' built-in visualization:

```python
# display() with a DataFrame automatically offers chart options in the notebook UI
display(
    df.groupBy("region", "month")
    .agg(F.sum("revenue").alias("total_revenue"))
    .orderBy("month")
)
# Then click the chart icon in the notebook output to configure visualization
```

### Alternative: plotly for interactive charts

```python
import plotly.express as px

pdf = result.toPandas()
fig = px.line(pdf, x="date", y="revenue", color="region", title="Revenue by Region")
display(fig)
```

---

## Script Conversion Template

### Before (R script)

```r
#!/usr/bin/env Rscript
# sales_report.R

library(dplyr)
library(readr)
library(ggplot2)

# Arguments
args <- commandArgs(trailingOnly = TRUE)
input_date <- args[1]
output_dir <- args[2]

# Read data
orders <- read_csv("data/orders.csv")
customers <- read_csv("data/customers.csv")

# Transform
report <- orders %>%
  filter(order_date >= input_date) %>%
  left_join(customers, by = "customer_id") %>%
  group_by(region, month = format(order_date, "%Y-%m")) %>%
  summarize(
    total_revenue = sum(amount),
    order_count = n(),
    avg_order = mean(amount),
    .groups = "drop"
  ) %>%
  arrange(region, month)

# Visualize
p <- ggplot(report, aes(x = month, y = total_revenue, fill = region)) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(title = "Monthly Revenue by Region")

ggsave(file.path(output_dir, "revenue_chart.png"), p)

# Write output
write_csv(report, file.path(output_dir, "sales_report.csv"))
```

### After (Databricks notebook)

```python
# Databricks notebook: sales_report

# 1. Imports
from pyspark.sql import functions as F
import matplotlib.pyplot as plt

# 2. Parameters (replaces commandArgs)
dbutils.widgets.text("input_date", "2024-01-01", "Start Date")
dbutils.widgets.text("catalog", "my_catalog", "Catalog")
dbutils.widgets.text("schema", "my_schema", "Schema")

input_date = dbutils.widgets.get("input_date")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# 3. Read data (replaces read_csv with Unity Catalog tables)
orders = spark.table(f"{catalog}.{schema}.orders")
customers = spark.table(f"{catalog}.{schema}.customers")

# 4. Transform (replaces dplyr pipeline)
report = (
    orders
    .filter(F.col("order_date") >= input_date)
    .join(customers, on="customer_id", how="left")
    .withColumn("month", F.date_format("order_date", "yyyy-MM"))
    .groupBy("region", "month")
    .agg(
        F.sum("amount").alias("total_revenue"),
        F.count("*").alias("order_count"),
        F.avg("amount").alias("avg_order"),
    )
    .orderBy("region", "month")
)

# 5. Visualize (replaces ggplot2)
pdf = report.toPandas()

fig, ax = plt.subplots(figsize=(14, 6))
regions = pdf["region"].unique()
bar_width = 0.8 / len(regions)
months = sorted(pdf["month"].unique())

for i, region in enumerate(regions):
    data = pdf[pdf["region"] == region].set_index("month").reindex(months).fillna(0)
    positions = [j + i * bar_width for j in range(len(months))]
    ax.bar(positions, data["total_revenue"], bar_width, label=region)

ax.set_xticks([j + bar_width * (len(regions) - 1) / 2 for j in range(len(months))])
ax.set_xticklabels(months, rotation=45)
ax.set_title("Monthly Revenue by Region")
ax.legend()
plt.tight_layout()
display(fig)

# 6. Write output (replaces write_csv with Delta table)
report.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.sales_report")

display(report)
```

---

## Testing Converted Code

### Strategy: Compare R and PySpark outputs

1. Run the original R script on a known dataset
2. Run the converted PySpark on the same dataset
3. Compare:
   - Row counts match
   - Column names and types match
   - Aggregate values match (sum, mean, min, max per group)
   - Sample rows match (spot-check 10-20 rows)

```python
# Quick validation template
r_count = 12345  # From R: nrow(result)
pyspark_count = result.count()
assert pyspark_count == r_count, f"Row count mismatch: R={r_count}, PySpark={pyspark_count}"

# Compare aggregates
r_total = 987654.32  # From R: sum(result$total_revenue)
pyspark_total = result.agg(F.sum("total_revenue")).collect()[0][0]
assert abs(pyspark_total - r_total) < 0.01, f"Total mismatch: R={r_total}, PySpark={pyspark_total}"
```

### Common conversion bugs to check

| Bug | Symptom | Fix |
|-----|---------|-----|
| Missing parentheses in filter conditions | Wrong row count | Add `()` around each `&`/`|` condition |
| 1-indexed vs 0-indexed | Off-by-one in array access | R arrays are 1-indexed; PySpark is 0-indexed |
| NULL handling difference | Different counts with nulls | Check `na.rm=TRUE` vs PySpark null behavior |
| Factor ordering | Different sort order | PySpark sorts strings lexicographically |
| Float precision | Small numeric differences | Use `round()` or tolerance in comparisons |
| Timezone handling | Timestamp differences | Set `spark.sql.session.timeZone` explicitly |

---

## Scheduling as a Databricks Job

Once converted, schedule the notebook as a production job:

```python
# Use the databricks-jobs skill for detailed job creation
# Basic pattern:
# 1. Create a job pointing to the notebook
# 2. Set parameters via job task parameters (maps to dbutils.widgets)
# 3. Schedule with cron expression
# 4. Configure alerts and retries
```

See the `databricks-jobs` skill for complete job configuration patterns.

---

## Why Migrate: Serverless Benefits

| Aspect | R on Classic Cluster | PySpark on Serverless |
|--------|---------------------|----------------------|
| **Startup time** | 5-10 minutes | Seconds |
| **Cost model** | Pay for cluster uptime | Pay per query/job |
| **Scaling** | Manual cluster sizing | Auto-scales |
| **Maintenance** | Manage R libraries, cluster configs | Zero infrastructure |
| **Concurrency** | Limited by cluster size | Elastic |
| **Supported** | Classic clusters only | Serverless, jobs, SQL warehouses |
