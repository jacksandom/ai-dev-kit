# Advanced Patterns: Window Functions, Reshaping, UDFs

Reference for converting complex R patterns to PySpark.

## Window Functions

### dplyr grouped mutate → PySpark Window

In R, `group_by() %>% mutate()` creates window-style calculations. In PySpark, these require explicit `Window` specifications.

**R:**
```r
df %>%
  group_by(customer_id) %>%
  arrange(order_date) %>%
  mutate(
    order_rank = row_number(),
    prev_amount = lag(amount, 1),
    next_amount = lead(amount, 1),
    running_total = cumsum(amount),
    running_avg = cummean(amount),
    pct_of_customer = amount / sum(amount)
  ) %>%
  ungroup()
```

**PySpark:**
```python
from pyspark.sql.window import Window

# Define window spec once, reuse
w_ordered = Window.partitionBy("customer_id").orderBy("order_date")
w_unordered = Window.partitionBy("customer_id")

df = (
    df
    .withColumn("order_rank", F.row_number().over(w_ordered))
    .withColumn("prev_amount", F.lag("amount", 1).over(w_ordered))
    .withColumn("next_amount", F.lead("amount", 1).over(w_ordered))
    .withColumn("running_total", F.sum("amount").over(w_ordered))
    .withColumn("running_avg", F.avg("amount").over(w_ordered))
    .withColumn("pct_of_customer", F.col("amount") / F.sum("amount").over(w_unordered))
)
```

> **Key insight:** R implicitly uses the grouping context. PySpark requires explicit window specs — `partitionBy` replaces `group_by`, `orderBy` replaces `arrange`.

### Window function mapping

| R Function | PySpark Equivalent | Requires orderBy? |
|------------|-------------------|-------------------|
| `row_number()` | `F.row_number().over(w)` | Yes |
| `rank()` | `F.rank().over(w)` | Yes |
| `dense_rank()` | `F.dense_rank().over(w)` | Yes |
| `percent_rank()` | `F.percent_rank().over(w)` | Yes |
| `cume_dist()` | `F.cume_dist().over(w)` | Yes |
| `ntile(n)` | `F.ntile(n).over(w)` | Yes |
| `lag(x, n)` | `F.lag("x", n).over(w)` | Yes |
| `lead(x, n)` | `F.lead("x", n).over(w)` | Yes |
| `cumsum(x)` | `F.sum("x").over(w)` | Yes |
| `cummean(x)` | `F.avg("x").over(w)` | Yes |
| `cummax(x)` | `F.max("x").over(w)` | Yes |
| `cummin(x)` | `F.min("x").over(w)` | Yes |
| `cumall(x)` | `F.min("x").over(w)` | Yes (boolean) |
| `cumany(x)` | `F.max("x").over(w)` | Yes (boolean) |

### Window frame control

**R:**
```r
# Sliding window: 3-row moving average
df %>%
  group_by(product) %>%
  arrange(date) %>%
  mutate(moving_avg = slider::slide_dbl(amount, mean, .before = 2, .after = 0))
```

**PySpark:**
```python
w_sliding = (
    Window.partitionBy("product")
    .orderBy("date")
    .rowsBetween(-2, 0)  # 2 rows before to current row
)
df = df.withColumn("moving_avg", F.avg("amount").over(w_sliding))
```

**Window frame options:**
- `rowsBetween(-N, M)` — N rows before to M rows after
- `rangeBetween(-N, M)` — by value range (useful for dates)
- `Window.unboundedPreceding` — from start of partition
- `Window.unboundedFollowing` — to end of partition
- `Window.currentRow` — current row (0)

### Top-N per group

**R:**
```r
df %>%
  group_by(category) %>%
  slice_max(order_by = revenue, n = 5)
```

**PySpark:**
```python
w = Window.partitionBy("category").orderBy(F.col("revenue").desc())
(
    df
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 5)
    .drop("rn")
)
```

---

## Reshaping Data

### pivot_wider (long → wide)

**R:**
```r
df %>% pivot_wider(
  names_from = quarter,
  values_from = revenue,
  values_fill = 0
)
```

**PySpark:**
```python
(
    df
    .groupBy("id")  # all columns NOT being pivoted
    .pivot("quarter")
    .agg(F.first("revenue"))
    .fillna(0)
)

# If you know the pivot values (faster):
(
    df
    .groupBy("id")
    .pivot("quarter", ["Q1", "Q2", "Q3", "Q4"])
    .agg(F.first("revenue"))
    .fillna(0)
)
```

### pivot_longer (wide → long)

**R:**
```r
df %>% pivot_longer(
  cols = c(Q1, Q2, Q3, Q4),
  names_to = "quarter",
  values_to = "revenue"
)
```

**PySpark:**
```python
# Option 1: stack() — most direct
df.selectExpr(
    "id",
    "stack(4, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3, 'Q4', Q4) as (quarter, revenue)"
).filter(F.col("revenue").isNotNull())

# Option 2: unpivot() — Spark 3.4+ / DBR 13.3+
df.unpivot(
    ids=["id"],
    values=["Q1", "Q2", "Q3", "Q4"],
    variableColumnName="quarter",
    valueColumnName="revenue"
)
```

### spread() / gather() (deprecated but still used)

**R:**
```r
# gather (wide → long) — old tidyr
df %>% gather(key = "metric", value = "value", -id, -name)

# spread (long → wide) — old tidyr
df %>% spread(key = metric, value = value)
```

**PySpark:**
```python
# gather → use unpivot or stack (same as pivot_longer)
value_cols = [c for c in df.columns if c not in ["id", "name"]]
df.unpivot(ids=["id", "name"], values=value_cols, variableColumnName="metric", valueColumnName="value")

# spread → use pivot (same as pivot_wider)
df.groupBy("id", "name").pivot("metric").agg(F.first("value"))
```

### separate() / unite()

**R:**
```r
# Split column
df %>% separate(full_name, into = c("first", "last"), sep = " ")

# Combine columns
df %>% unite(full_name, first, last, sep = " ")
```

**PySpark:**
```python
# separate — split string into columns
df = (
    df
    .withColumn("_parts", F.split("full_name", " "))
    .withColumn("first", F.col("_parts")[0])
    .withColumn("last", F.col("_parts")[1])
    .drop("_parts")
)

# unite — concatenate columns
df = df.withColumn("full_name", F.concat_ws(" ", F.col("first"), F.col("last")))
```

---

## Apply Patterns and UDFs

### Decision tree: when to use what

1. **Native PySpark functions** (always prefer) — fastest, runs in JVM
2. **`@pandas_udf`** (when native isn't possible) — vectorized, runs on batches
3. **`applyInPandas()`** (for grouped operations) — pandas DataFrame per group
4. **Python UDF `@udf`** (last resort) — row-by-row, serialization overhead

### sapply / lapply → native PySpark

**R:**
```r
# Simple transformation — use native functions
df$clean_text <- sapply(df$text, function(x) {
  x <- trimws(x)
  x <- tolower(x)
  x <- gsub("[^a-z0-9 ]", "", x)
  return(x)
})
```

**PySpark (native — preferred):**
```python
df = df.withColumn("clean_text",
    F.regexp_replace(F.lower(F.trim(F.col("text"))), "[^a-z0-9 ]", "")
)
```

### sapply with complex logic → @pandas_udf

**R:**
```r
df$sentiment <- sapply(df$text, function(x) {
  # Complex NLP logic
  score <- compute_sentiment(x)
  return(score)
})
```

**PySpark:**
```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("double")
def sentiment_udf(texts: pd.Series) -> pd.Series:
    return texts.apply(compute_sentiment)

df = df.withColumn("sentiment", sentiment_udf(F.col("text")))
```

### apply with grouped operations → applyInPandas

**R:**
```r
# Custom function per group
results <- df %>%
  group_by(region) %>%
  group_modify(~ {
    model <- lm(revenue ~ marketing_spend, data = .x)
    tibble(
      slope = coef(model)[2],
      r_squared = summary(model)$r.squared
    )
  })
```

**PySpark:**
```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pandas as pd
from sklearn.linear_model import LinearRegression

output_schema = StructType([
    StructField("region", StringType()),
    StructField("slope", DoubleType()),
    StructField("r_squared", DoubleType()),
])

def fit_model(pdf: pd.DataFrame) -> pd.DataFrame:
    model = LinearRegression()
    X = pdf[["marketing_spend"]]
    y = pdf["revenue"]
    model.fit(X, y)
    return pd.DataFrame({
        "region": [pdf["region"].iloc[0]],
        "slope": [model.coef_[0]],
        "r_squared": [model.score(X, y)],
    })

result = df.groupBy("region").applyInPandas(fit_model, schema=output_schema)
```

### tapply / mapply → groupBy + agg or applyInPandas

**R:**
```r
# tapply — apply function by group
tapply(df$amount, df$region, mean)
```

**PySpark:**
```python
df.groupBy("region").agg(F.avg("amount"))
```

---

## R-Specific Idiom Conversions

### Vectorized operations

**R:**
```r
# R is vectorized by default
df$total <- df$price * df$quantity
df$flag <- df$amount > 1000
```

**PySpark:**
```python
# PySpark column expressions are also vectorized
df = df.withColumn("total", F.col("price") * F.col("quantity"))
df = df.withColumn("flag", F.col("amount") > 1000)
```

### which() / match()

**R:**
```r
# Find rows matching condition
high_value <- df[which(df$amount > 10000), ]

# Match values
df$category_id <- match(df$category, c("A", "B", "C"))
```

**PySpark:**
```python
# which() → filter
high_value = df.filter(F.col("amount") > 10000)

# match() → create mapping
from pyspark.sql.functions import create_map, lit
mapping = create_map([val for pair in [("A", 1), ("B", 2), ("C", 3)] for val in (F.lit(pair[0]), F.lit(pair[1]))])
df = df.withColumn("category_id", mapping[F.col("category")])
```

### Nested data / list columns

**R:**
```r
df %>%
  group_by(customer_id) %>%
  summarize(orders = list(order_id))
```

**PySpark:**
```python
df.groupBy("customer_id").agg(F.collect_list("order_id").alias("orders"))

# Or collect unique values:
df.groupBy("customer_id").agg(F.collect_set("order_id").alias("unique_orders"))
```

### Working with arrays (from collect_list or split)

| R Operation | PySpark Equivalent |
|-------------|-------------------|
| `length(x)` | `F.size("x")` |
| `x[1]` | `F.col("x")[0]` (0-indexed!) |
| `x %in% y` | `F.array_contains("y", F.col("x"))` |
| `sort(x)` | `F.sort_array("x")` |
| `unique(x)` | `F.array_distinct("x")` |
| `intersect(x, y)` | `F.array_intersect("x", "y")` |
| `union(x, y)` | `F.array_union("x", "y")` |
| `unlist(x)` | `F.explode("x")` |

---

## Performance Tips for Converted Code

1. **Avoid `.collect()` and `.toPandas()`** in the middle of pipelines — these pull data to the driver
2. **Never iterate rows with `for` loops** — use column expressions instead
3. **Replace UDFs with native functions** wherever possible — even a 3-step native expression beats a simple UDF
4. **Use `F.broadcast()` for small table joins** — replaces R's merge with small lookup tables
5. **Cache intermediate results** if reused: `df.cache()` — but only if the DataFrame is used more than once
6. **Repartition before heavy joins**: `df.repartition("join_key")` — improves shuffle performance
