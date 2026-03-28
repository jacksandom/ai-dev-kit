---
name: r-to-pyspark
description: "Converts R code (dplyr, tidyr, ggplot2, data.table, base R) to idiomatic PySpark for Databricks. Use when migrating R scripts, translating R data manipulation to PySpark DataFrames, or replacing R workloads with serverless-compatible PySpark."
---

# R to PySpark Conversion

## Overview

Convert R code to idiomatic PySpark that runs on Databricks. Covers the full R ecosystem: dplyr/tidyverse, data.table, base R, ggplot2, lubridate, stringr, and statistical functions. The converted code runs on serverless compute (R cannot).

## When to Use This Skill

- User asks to "convert R to PySpark" or "migrate R to Databricks"
- User provides R code and wants a PySpark equivalent
- User mentions dplyr, tidyverse, tidyr, ggplot2, data.table, or lubridate in a Databricks context
- User wants to replace R workloads to enable serverless compute
- User asks about R package equivalents in PySpark/Python

## Critical Rules

1. **Always produce PySpark DataFrame API** — never pandas, pandas-on-Spark, or base Python unless explicitly requested
2. **Use `from pyspark.sql import functions as F`** and reference columns as `F.col("name")` in expressions
3. **Unity Catalog 3-level namespace** — always use `catalog.schema.table` for table references
4. **Use `spark.read` / `spark.table()`** for data I/O — never local file paths like `read.csv("/local/path")`
5. **Use `display()`** for notebook visualization — never `.show()` or `print(df)`
6. **Prefer native PySpark functions over UDFs** — UDFs serialize data to Python and kill performance
7. **Preserve the logic, not the syntax** — R's vectorized style maps to PySpark column expressions, not row-by-row loops
8. **Handle NULL differences** — R uses `NA` per type (`NA_real_`, `NA_character_`); PySpark uses `None`/`null` universally. Use `F.isnull()` or `F.coalesce()` instead of R's `is.na()`

## Quick Start

**R code:**
```r
library(dplyr)

result <- df %>%
  filter(status == "active", amount > 100) %>%
  mutate(tax = amount * 0.08, total = amount + tax) %>%
  group_by(region) %>%
  summarize(
    avg_total = mean(total, na.rm = TRUE),
    order_count = n()
  ) %>%
  arrange(desc(avg_total))
```

**PySpark equivalent:**
```python
from pyspark.sql import functions as F

result = (
    df
    .filter((F.col("status") == "active") & (F.col("amount") > 100))
    .withColumn("tax", F.col("amount") * 0.08)
    .withColumn("total", F.col("amount") + F.col("tax"))
    .groupBy("region")
    .agg(
        F.avg("total").alias("avg_total"),
        F.count("*").alias("order_count"),
    )
    .orderBy(F.col("avg_total").desc())
)

display(result)
```

**Key conversions shown:**
- `%>%` pipe → method chaining with parentheses
- `filter()` → `.filter()` with `&` / `|` operators (each condition in parentheses)
- `mutate()` → `.withColumn()` per new column
- `group_by() %>% summarize()` → `.groupBy().agg()`
- `n()` → `F.count("*")`
- `mean(x, na.rm=TRUE)` → `F.avg("x")` (PySpark ignores nulls by default)
- `arrange(desc())` → `.orderBy(F.col().desc())`

## Quick Reference Table

| R | PySpark | Notes |
|---|---------|-------|
| `%>%` | Method chaining | Wrap in `()` for multiline |
| `filter(cond1, cond2)` | `.filter((cond1) & (cond2))` | Parentheses around each condition |
| `select(col1, col2)` | `.select("col1", "col2")` | |
| `mutate(new = expr)` | `.withColumn("new", expr)` | One call per column |
| `rename(new = old)` | `.withColumnRenamed("old", "new")` | |
| `arrange(desc(x))` | `.orderBy(F.col("x").desc())` | |
| `group_by(x) %>% summarize(...)` | `.groupBy("x").agg(...)` | |
| `n()` | `F.count("*")` | Inside `.agg()` |
| `mean(x)` | `F.avg("x")` | Ignores nulls by default |
| `sum(x)` | `F.sum("x")` | |
| `left_join(y, by="key")` | `.join(y, on="key", how="left")` | |
| `bind_rows(a, b)` | `a.unionByName(b)` | Use `allowMissingColumns=True` if schemas differ |
| `distinct()` | `.distinct()` | Or `.dropDuplicates(["col"])` |
| `case_when(x>0 ~ "pos", TRUE ~ "neg")` | `F.when(F.col("x")>0, "pos").otherwise("neg")` | |
| `ifelse(cond, a, b)` | `F.when(cond, a).otherwise(b)` | |
| `is.na(x)` | `F.isnull("x")` | Or `F.col("x").isNull()` |
| `replace_na(x, 0)` | `F.coalesce(F.col("x"), F.lit(0))` | Or `.fillna({"x": 0})` |
| `pull(col)` | `.select("col").rdd.flatMap(lambda x: x).collect()` | Avoid — collect is expensive |
| `paste0(a, b)` | `F.concat(F.col("a"), F.col("b"))` | |
| `nrow(df)` | `df.count()` | |
| `head(df, 10)` | `df.limit(10)` | |

## Common Patterns

### Pattern 1: Multiple mutate columns

**R:**
```r
df %>% mutate(
  full_name = paste(first, last),
  age_group = case_when(age < 18 ~ "minor", age < 65 ~ "adult", TRUE ~ "senior"),
  active_flag = ifelse(status == "active", 1, 0)
)
```

**PySpark:**
```python
df = (
    df
    .withColumn("full_name", F.concat_ws(" ", F.col("first"), F.col("last")))
    .withColumn("age_group",
        F.when(F.col("age") < 18, "minor")
         .when(F.col("age") < 65, "adult")
         .otherwise("senior")
    )
    .withColumn("active_flag",
        F.when(F.col("status") == "active", 1).otherwise(0)
    )
)
```

### Pattern 2: Grouped aggregation with multiple summaries

**R:**
```r
df %>%
  group_by(department, year) %>%
  summarize(
    total_sales = sum(amount),
    avg_sales = mean(amount),
    num_orders = n(),
    max_order = max(amount),
    .groups = "drop"
  )
```

**PySpark:**
```python
(
    df
    .groupBy("department", "year")
    .agg(
        F.sum("amount").alias("total_sales"),
        F.avg("amount").alias("avg_sales"),
        F.count("*").alias("num_orders"),
        F.max("amount").alias("max_order"),
    )
)
```

### Pattern 3: Reading data and writing results

**R:**
```r
raw <- read.csv("data/sales.csv")
result <- raw %>% filter(year == 2024) %>% group_by(region) %>% summarize(total = sum(amount))
write.csv(result, "output/summary.csv")
```

**PySpark:**
```python
raw = spark.read.csv("/Volumes/catalog/schema/volume/sales.csv", header=True, inferSchema=True)
result = (
    raw
    .filter(F.col("year") == 2024)
    .groupBy("region")
    .agg(F.sum("amount").alias("total"))
)
result.write.mode("overwrite").saveAsTable("catalog.schema.sales_summary")
```

### Pattern 4: Window functions

**R:**
```r
df %>%
  group_by(customer_id) %>%
  mutate(
    order_rank = row_number(),
    prev_amount = lag(amount),
    running_total = cumsum(amount)
  ) %>%
  ungroup()
```

**PySpark:**
```python
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date")

df = (
    df
    .withColumn("order_rank", F.row_number().over(w))
    .withColumn("prev_amount", F.lag("amount").over(w))
    .withColumn("running_total", F.sum("amount").over(w))
)
```

### Pattern 5: ggplot2 visualization conversion

For ggplot2, convert to matplotlib with `display()` for Databricks notebooks. Always convert the Spark DataFrame to pandas first (small aggregated data only).

**R:**
```r
library(ggplot2)
ggplot(summary_df, aes(x = month, y = revenue, color = region)) +
  geom_line() + geom_point() +
  labs(title = "Revenue by Region", x = "Month", y = "Revenue ($)")
```

**PySpark:**
```python
import matplotlib.pyplot as plt

# Convert small aggregated DataFrame to pandas for plotting
pdf = summary_df.toPandas()

fig, ax = plt.subplots(figsize=(12, 6))
for region in pdf["region"].unique():
    data = pdf[pdf["region"] == region].sort_values("month")
    ax.plot(data["month"], data["revenue"], marker="o", label=region)

ax.set_title("Revenue by Region")
ax.set_xlabel("Month")
ax.set_ylabel("Revenue ($)")
ax.legend()
plt.tight_layout()
display(fig)  # Databricks notebook rendering
```

> **Rules for visualization conversion:**
> - Always use `matplotlib.pyplot` as the primary target for ggplot2
> - Always call `display(fig)` — not `plt.show()` — for Databricks notebook rendering
> - Always call `.toPandas()` to convert the DataFrame before plotting
> - For simple charts, Databricks native `display(df)` is also acceptable
> - `geom_line` → `ax.plot()`, `geom_point` → `marker="o"` param, `geom_bar` → `ax.bar()`, `labs` → `set_title/set_xlabel/set_ylabel`

### Pattern 6: Complete script conversion template

When converting a full R script, follow this structure:

```python
# 1. Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 2. Configuration (replaces R command-line args / config files)
catalog = dbutils.widgets.get("catalog")  # or hardcode for notebooks
schema = dbutils.widgets.get("schema")

# 3. Read data (replaces read.csv / readRDS / dbReadTable)
orders = spark.table(f"{catalog}.{schema}.orders")
customers = spark.table(f"{catalog}.{schema}.customers")

# 4. Transform (replaces dplyr pipeline)
result = (
    orders
    .join(customers, on="customer_id", how="left")
    .filter(F.col("order_date") >= "2024-01-01")
    .withColumn("order_month", F.date_trunc("month", F.col("order_date")))
    .groupBy("order_month", "region")
    .agg(
        F.sum("amount").alias("total_revenue"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
    .orderBy("order_month", "region")
)

# 5. Write output (replaces write.csv / saveRDS)
result.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.monthly_summary")

# 6. Display (replaces print / View)
display(result)
```

## Common Pitfalls

| R Idiom | Problem | PySpark Solution |
|---------|---------|-----------------|
| `df$column` | Column access syntax doesn't exist | `F.col("column")` or `df["column"]` |
| `sapply(df$x, custom_fn)` | Row-by-row is slow at scale | Use native functions or `@pandas_udf` |
| `for (i in 1:nrow(df))` | Row iteration doesn't scale | Use column expressions or `applyInPandas` |
| `factor()` levels | PySpark has no factor type | Use `StringType` + ordered window functions |
| `Sys.time()` for timing | Measures driver only | Use Spark UI metrics instead |
| `source("helpers.R")` | No direct equivalent | Use `%run ./helpers` in notebooks or Python imports |
| `library(parallel)` | R parallel != Spark parallel | Spark parallelizes natively across executors |
| `readRDS()` / `saveRDS()` | R-specific binary format | Use Delta tables or Parquet |
| `tibble` with list columns | Nested structures differ | Use `StructType` or `ArrayType` |
| `NA_real_` vs `NA_character_` | Typed NAs don't exist in PySpark | All nulls are untyped `null` |

## Reference Files

- [1-data-manipulation.md](1-data-manipulation.md) - Complete dplyr verb mappings with before/after examples
- [2-data-io.md](2-data-io.md) - Reading/writing data: CSV, Parquet, Delta, Unity Catalog, Volumes
- [3-functions-and-types.md](3-functions-and-types.md) - String, date, math, stats function mappings and type conversions
- [4-advanced-patterns.md](4-advanced-patterns.md) - Window functions, reshaping, UDFs, apply patterns
- [5-databricks-migration.md](5-databricks-migration.md) - End-to-end migration guide: R scripts to Databricks notebooks/jobs

## Related Skills

- `databricks-spark-declarative-pipelines` — if converting R ETL into streaming tables / materialized views
- `databricks-unity-catalog` — for catalog/schema/volume setup
- `databricks-dbsql` — if the R code is doing SQL-like analytics that map better to SQL
- `databricks-jobs` — for scheduling converted notebooks as production jobs
- `databricks-synthetic-data-gen` — for generating test data to validate conversions
