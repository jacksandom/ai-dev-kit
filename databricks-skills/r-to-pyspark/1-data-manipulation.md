# Data Manipulation: dplyr to PySpark

Complete reference for converting dplyr/tidyverse data manipulation to PySpark DataFrame operations.

## Setup

**R:**
```r
library(dplyr)
library(tidyr)
```

**PySpark:**
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
```

---

## Filtering Rows

### filter() / subset()

**R:**
```r
# Single condition
df %>% filter(age > 30)

# Multiple conditions (AND)
df %>% filter(age > 30, status == "active")

# OR condition
df %>% filter(age > 30 | status == "active")

# %in% operator
df %>% filter(region %in% c("US", "UK", "DE"))

# Negation
df %>% filter(!is.na(email))
```

**PySpark:**
```python
# Single condition
df.filter(F.col("age") > 30)

# Multiple conditions (AND) — each in parentheses
df.filter((F.col("age") > 30) & (F.col("status") == "active"))

# OR condition
df.filter((F.col("age") > 30) | (F.col("status") == "active"))

# isin (replaces %in%)
df.filter(F.col("region").isin(["US", "UK", "DE"]))

# Negation / not null
df.filter(F.col("email").isNotNull())
```

> **Key difference:** PySpark requires parentheses around each condition when using `&` or `|`. Forgetting them is the #1 conversion bug.

---

## Selecting Columns

### select() / rename()

**R:**
```r
# Select specific columns
df %>% select(name, age, email)

# Select with rename
df %>% select(customer_name = name, customer_age = age)

# Drop columns
df %>% select(-temp_col, -debug_col)

# Select by pattern
df %>% select(starts_with("sales_"))
df %>% select(ends_with("_date"))
df %>% select(matches("^amt_\\d+"))
```

**PySpark:**
```python
# Select specific columns
df.select("name", "age", "email")

# Select with rename
df.select(
    F.col("name").alias("customer_name"),
    F.col("age").alias("customer_age"),
)

# Drop columns
df.drop("temp_col", "debug_col")

# Select by pattern — use list comprehension
df.select([c for c in df.columns if c.startswith("sales_")])
df.select([c for c in df.columns if c.endswith("_date")])
import re
df.select([c for c in df.columns if re.match(r"^amt_\d+", c)])
```

### rename()

**R:**
```r
df %>% rename(new_name = old_name, another = original)
```

**PySpark:**
```python
df.withColumnRenamed("old_name", "new_name").withColumnRenamed("original", "another")

# Or rename many at once:
rename_map = {"old_name": "new_name", "original": "another"}
for old, new in rename_map.items():
    df = df.withColumnRenamed(old, new)
```

---

## Creating / Transforming Columns

### mutate()

**R:**
```r
df %>% mutate(
  total = price * quantity,
  discount_price = price * (1 - discount_rate),
  category_upper = toupper(category)
)
```

**PySpark:**
```python
df = (
    df
    .withColumn("total", F.col("price") * F.col("quantity"))
    .withColumn("discount_price", F.col("price") * (1 - F.col("discount_rate")))
    .withColumn("category_upper", F.upper(F.col("category")))
)
```

### transmute() (select only new columns)

**R:**
```r
df %>% transmute(
  id = id,
  total = price * quantity
)
```

**PySpark:**
```python
df.select(
    "id",
    (F.col("price") * F.col("quantity")).alias("total"),
)
```

### across() (apply function to multiple columns)

**R:**
```r
df %>% mutate(across(c(price, cost, revenue), ~ round(., 2)))
df %>% mutate(across(where(is.character), toupper))
```

**PySpark:**
```python
# Round specific columns
for col_name in ["price", "cost", "revenue"]:
    df = df.withColumn(col_name, F.round(F.col(col_name), 2))

# Upper-case all string columns
from pyspark.sql.types import StringType
string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
for col_name in string_cols:
    df = df.withColumn(col_name, F.upper(F.col(col_name)))
```

---

## Sorting

### arrange()

**R:**
```r
df %>% arrange(region, desc(total_sales))
```

**PySpark:**
```python
df.orderBy("region", F.col("total_sales").desc())

# Alternative
df.orderBy(F.asc("region"), F.desc("total_sales"))
```

---

## Grouping and Aggregation

### group_by() + summarize()

**R:**
```r
df %>%
  group_by(region, year) %>%
  summarize(
    total = sum(amount, na.rm = TRUE),
    avg_amount = mean(amount, na.rm = TRUE),
    count = n(),
    unique_customers = n_distinct(customer_id),
    max_order = max(amount, na.rm = TRUE),
    min_order = min(amount, na.rm = TRUE),
    first_date = min(order_date),
    .groups = "drop"
  )
```

**PySpark:**
```python
(
    df
    .groupBy("region", "year")
    .agg(
        F.sum("amount").alias("total"),
        F.avg("amount").alias("avg_amount"),
        F.count("*").alias("count"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.max("amount").alias("max_order"),
        F.min("amount").alias("min_order"),
        F.min("order_date").alias("first_date"),
    )
)
```

> **Note:** PySpark aggregation functions ignore nulls by default — no `na.rm = TRUE` equivalent needed.

### count() / tally()

**R:**
```r
df %>% count(region, sort = TRUE)
df %>% group_by(region) %>% tally()
```

**PySpark:**
```python
df.groupBy("region").count().orderBy(F.col("count").desc())
```

### Grouped mutate (add aggregation as column without collapsing)

**R:**
```r
df %>%
  group_by(region) %>%
  mutate(pct_of_region = amount / sum(amount)) %>%
  ungroup()
```

**PySpark:**
```python
w = Window.partitionBy("region")
df = df.withColumn("pct_of_region", F.col("amount") / F.sum("amount").over(w))
```

---

## Joins

### *_join()

**R:**
```r
left_join(orders, customers, by = "customer_id")
left_join(orders, customers, by = c("cust_id" = "id"))
inner_join(a, b, by = c("key1", "key2"))
anti_join(orders, blacklist, by = "customer_id")
```

**PySpark:**
```python
# Same column name
orders.join(customers, on="customer_id", how="left")

# Different column names
orders.join(customers, orders["cust_id"] == customers["id"], how="left")

# Multiple keys
a.join(b, on=["key1", "key2"], how="inner")

# Anti join
orders.join(blacklist, on="customer_id", how="left_anti")
```

**Join type mapping:**

| R | PySpark `how=` |
|---|----------------|
| `left_join()` | `"left"` |
| `right_join()` | `"right"` |
| `inner_join()` | `"inner"` |
| `full_join()` | `"full"` or `"outer"` |
| `semi_join()` | `"left_semi"` |
| `anti_join()` | `"left_anti"` |
| `cross_join()` | `"cross"` |

### Handling duplicate column names after join

**R:**
```r
left_join(a, b, by = "key") %>% select(-ends_with(".y")) %>% rename_with(~ gsub(".x$", "", .), ends_with(".x"))
```

**PySpark:**
```python
# Drop the duplicate column from the right table after join
result = a.join(b, on="key", how="left").drop(b["duplicate_col"])
```

---

## Combining DataFrames

### bind_rows() / bind_cols()

**R:**
```r
bind_rows(df1, df2)
bind_rows(df1, df2, .id = "source")
bind_cols(df1, df2)
```

**PySpark:**
```python
# bind_rows — same schema
df1.unionByName(df2)

# bind_rows — different schemas (fills missing with null)
df1.unionByName(df2, allowMissingColumns=True)

# bind_cols — no direct equivalent; use row index join (avoid if possible)
from pyspark.sql.window import Window
w = Window.orderBy(F.monotonically_increasing_id())
df1_idx = df1.withColumn("_idx", F.row_number().over(w))
df2_idx = df2.withColumn("_idx", F.row_number().over(w))
df1_idx.join(df2_idx, on="_idx").drop("_idx")
```

---

## Distinct and Duplicates

**R:**
```r
df %>% distinct()
df %>% distinct(customer_id, .keep_all = TRUE)
df %>% group_by(customer_id) %>% slice_head(n = 1)
```

**PySpark:**
```python
# All columns distinct
df.distinct()

# Distinct by specific columns, keep first occurrence
df.dropDuplicates(["customer_id"])

# Keep specific row per group (e.g., most recent)
w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc())
df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
```

---

## Conditional Logic

### case_when() / if_else()

**R:**
```r
df %>% mutate(
  tier = case_when(
    revenue >= 1000000 ~ "enterprise",
    revenue >= 100000  ~ "mid_market",
    revenue >= 10000   ~ "smb",
    TRUE               ~ "starter"
  )
)
```

**PySpark:**
```python
df = df.withColumn("tier",
    F.when(F.col("revenue") >= 1000000, "enterprise")
     .when(F.col("revenue") >= 100000, "mid_market")
     .when(F.col("revenue") >= 10000, "smb")
     .otherwise("starter")
)
```

> **Key mapping:** R's `TRUE ~ "default"` maps to `.otherwise("default")`.

---

## NULL / NA Handling

| R | PySpark |
|---|---------|
| `is.na(x)` | `F.col("x").isNull()` or `F.isnull("x")` |
| `!is.na(x)` | `F.col("x").isNotNull()` |
| `replace_na(list(x=0))` | `df.fillna({"x": 0})` |
| `coalesce(x, y)` | `F.coalesce(F.col("x"), F.col("y"))` |
| `na.omit(df)` | `df.dropna()` |
| `drop_na(x, y)` | `df.dropna(subset=["x", "y"])` |
| `complete.cases(df)` | `df.dropna(how="any")` |

> **Important:** In R, `NA == NA` returns `NA`. In PySpark, `null == null` returns `null` (falsy). Use `.isNull()` for null checks, never `== None`.

---

## Pipe Chaining Pattern

The R pipe `%>%` maps to PySpark method chaining. Wrap in parentheses for multiline:

**R:**
```r
result <- df %>%
  filter(year == 2024) %>%
  mutate(margin = revenue - cost) %>%
  group_by(product) %>%
  summarize(total_margin = sum(margin)) %>%
  arrange(desc(total_margin)) %>%
  head(10)
```

**PySpark:**
```python
result = (
    df
    .filter(F.col("year") == 2024)
    .withColumn("margin", F.col("revenue") - F.col("cost"))
    .groupBy("product")
    .agg(F.sum("margin").alias("total_margin"))
    .orderBy(F.col("total_margin").desc())
    .limit(10)
)
```

The parenthesized multiline style is the PySpark equivalent of R's pipe — each line is one transformation step, read top to bottom.
