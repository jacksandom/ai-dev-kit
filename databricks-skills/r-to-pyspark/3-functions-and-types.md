# Functions and Types: R to PySpark Mapping

Complete reference for converting R functions (stringr, lubridate, base R math/stats) to PySpark equivalents, plus type system mapping.

## String Functions

### stringr / base R → PySpark

| R Function | PySpark Equivalent | Example |
|------------|-------------------|---------|
| `paste0(a, b)` | `F.concat(F.col("a"), F.col("b"))` | |
| `paste(a, b, sep="-")` | `F.concat_ws("-", F.col("a"), F.col("b"))` | |
| `str_detect(x, "pat")` | `F.col("x").rlike("pat")` | Returns boolean column |
| `str_replace(x, "old", "new")` | `F.regexp_replace("x", "old", "new")` | First match only in R; all in PySpark |
| `str_replace_all(x, "old", "new")` | `F.regexp_replace("x", "old", "new")` | Same — PySpark replaces all by default |
| `gsub("old", "new", x)` | `F.regexp_replace("x", "old", "new")` | |
| `sub("old", "new", x)` | Use `F.regexp_replace` with anchored pattern | PySpark has no "first only" replace |
| `str_extract(x, "(\\d+)")` | `F.regexp_extract("x", r"(\d+)", 1)` | Group index 1 |
| `str_sub(x, 1, 5)` | `F.substring("x", 1, 5)` | 1-indexed in both |
| `substr(x, 1, 5)` | `F.substring("x", 1, 5)` | |
| `nchar(x)` | `F.length("x")` | |
| `str_length(x)` | `F.length("x")` | |
| `toupper(x)` | `F.upper("x")` | |
| `tolower(x)` | `F.lower("x")` | |
| `str_to_title(x)` | `F.initcap("x")` | |
| `trimws(x)` | `F.trim("x")` | |
| `str_trim(x)` | `F.trim("x")` | |
| `str_pad(x, 10, "left", "0")` | `F.lpad("x", 10, "0")` | |
| `str_split(x, ",")` | `F.split("x", ",")` | Returns ArrayType |
| `str_c(a, b, sep="")` | `F.concat(F.col("a"), F.col("b"))` | |
| `str_starts(x, "pre")` | `F.col("x").startswith("pre")` | |
| `str_ends(x, "suf")` | `F.col("x").endswith("suf")` | |
| `str_count(x, "a")` | `F.size(F.split("x", "a")) - 1` | Workaround |
| `str_wrap(x, width=80)` | No direct equivalent | Handle in display layer |
| `sprintf("%.2f", x)` | `F.format_number("x", 2)` | |
| `grepl("pat", x)` | `F.col("x").rlike("pat")` | |

### String examples

**R:**
```r
df %>% mutate(
  domain = str_extract(email, "@(.+)$"),
  name_clean = str_to_title(trimws(name)),
  code = str_pad(id, 8, "left", "0")
)
```

**PySpark:**
```python
df = (
    df
    .withColumn("domain", F.regexp_extract("email", r"@(.+)$", 1))
    .withColumn("name_clean", F.initcap(F.trim(F.col("name"))))
    .withColumn("code", F.lpad(F.col("id").cast("string"), 8, "0"))
)
```

---

## Date and Time Functions

### lubridate / base R → PySpark

| R Function | PySpark Equivalent | Notes |
|------------|-------------------|-------|
| `ymd("2024-01-15")` | `F.to_date(F.lit("2024-01-15"))` | Default yyyy-MM-dd |
| `mdy("01/15/2024")` | `F.to_date(F.lit("01/15/2024"), "MM/dd/yyyy")` | Specify format |
| `dmy("15-01-2024")` | `F.to_date(F.lit("15-01-2024"), "dd-MM-yyyy")` | |
| `ymd_hms("2024-01-15 10:30:00")` | `F.to_timestamp(F.lit("2024-01-15 10:30:00"))` | |
| `as.Date(x)` | `F.to_date("x")` | |
| `as.POSIXct(x)` | `F.to_timestamp("x")` | |
| `year(x)` | `F.year("x")` | |
| `month(x)` | `F.month("x")` | |
| `day(x)` / `mday(x)` | `F.dayofmonth("x")` | |
| `wday(x)` | `F.dayofweek("x")` | Sunday=1 in both |
| `yday(x)` | `F.dayofyear("x")` | |
| `hour(x)` | `F.hour("x")` | |
| `minute(x)` | `F.minute("x")` | |
| `second(x)` | `F.second("x")` | |
| `quarter(x)` | `F.quarter("x")` | |
| `floor_date(x, "month")` | `F.date_trunc("month", "x")` | |
| `ceiling_date(x, "month")` | `F.date_trunc("month", F.add_months("x", 1))` | No direct ceiling |
| `difftime(d1, d2, units="days")` | `F.datediff("d1", "d2")` | Returns integer days |
| `x + days(30)` | `F.date_add("x", 30)` | |
| `x + months(3)` | `F.add_months("x", 3)` | |
| `x - years(1)` | `F.add_months("x", -12)` | No `add_years` |
| `today()` | `F.current_date()` | |
| `now()` | `F.current_timestamp()` | |
| `format(x, "%Y-%m")` | `F.date_format("x", "yyyy-MM")` | Java date format strings |
| `make_date(y, m, d)` | `F.make_date("y", "m", "d")` | Spark 3.3+ |
| `interval(d1, d2)` | `F.datediff("d2", "d1")` | Returns days as integer |

### Date format string differences

| R (`strftime`) | PySpark (Java) | Meaning |
|----------------|----------------|---------|
| `%Y` | `yyyy` | 4-digit year |
| `%m` | `MM` | Month (01-12) |
| `%d` | `dd` | Day (01-31) |
| `%H` | `HH` | Hour 24h (00-23) |
| `%M` | `mm` | Minute (00-59) |
| `%S` | `ss` | Second (00-59) |
| `%B` | `MMMM` | Full month name |
| `%b` | `MMM` | Abbreviated month |
| `%A` | `EEEE` | Full weekday name |

### Date examples

**R:**
```r
df %>% mutate(
  order_month = floor_date(order_date, "month"),
  days_since = as.numeric(difftime(today(), order_date, units = "days")),
  next_review = order_date + months(6),
  formatted = format(order_date, "%B %Y")
)
```

**PySpark:**
```python
df = (
    df
    .withColumn("order_month", F.date_trunc("month", F.col("order_date")))
    .withColumn("days_since", F.datediff(F.current_date(), F.col("order_date")))
    .withColumn("next_review", F.add_months(F.col("order_date"), 6))
    .withColumn("formatted", F.date_format(F.col("order_date"), "MMMM yyyy"))
)
```

---

## Math and Statistical Functions

### Aggregate functions (inside group_by/summarize)

| R Function | PySpark Equivalent | Notes |
|------------|-------------------|-------|
| `sum(x, na.rm=T)` | `F.sum("x")` | Ignores nulls by default |
| `mean(x, na.rm=T)` | `F.avg("x")` | |
| `median(x)` | `F.percentile_approx("x", 0.5)` | Approximate; use `F.median("x")` on DBR 14.0+ |
| `sd(x)` | `F.stddev("x")` | Sample stddev (same as R default) |
| `var(x)` | `F.variance("x")` | Sample variance |
| `min(x)` | `F.min("x")` | |
| `max(x)` | `F.max("x")` | |
| `n()` | `F.count("*")` | |
| `n_distinct(x)` | `F.countDistinct("x")` | |
| `first(x)` | `F.first("x")` | |
| `last(x)` | `F.last("x")` | |
| `cor(x, y)` | `F.corr("x", "y")` | Pearson correlation |
| `quantile(x, 0.95)` | `F.percentile_approx("x", 0.95)` | |
| `IQR(x)` | `F.percentile_approx("x", 0.75) - F.percentile_approx("x", 0.25)` | |

### Scalar math functions (inside mutate)

| R Function | PySpark Equivalent |
|------------|-------------------|
| `abs(x)` | `F.abs("x")` |
| `sqrt(x)` | `F.sqrt("x")` |
| `log(x)` | `F.log("x")` | Natural log |
| `log2(x)` | `F.log2("x")` |
| `log10(x)` | `F.log10("x")` |
| `exp(x)` | `F.exp("x")` |
| `ceiling(x)` | `F.ceil("x")` |
| `floor(x)` | `F.floor("x")` |
| `round(x, 2)` | `F.round("x", 2)` |
| `sign(x)` | `F.signum("x")` |
| `x %% y` (modulo) | `F.col("x") % F.col("y")` |
| `x %/% y` (integer div) | `(F.col("x") / F.col("y")).cast("int")` |
| `pmin(x, y)` | `F.least("x", "y")` |
| `pmax(x, y)` | `F.greatest("x", "y")` |
| `cumsum(x)` | `F.sum("x").over(w)` | Requires Window |
| `cumprod(x)` | No direct — use `F.exp(F.sum(F.log("x")).over(w))` | |
| `cummax(x)` | `F.max("x").over(w)` | Requires Window |
| `cummin(x)` | `F.min("x").over(w)` | Requires Window |

### Statistical functions without direct PySpark equivalents

These R functions require `@pandas_udf` or Python libraries:

| R Function | Recommended Approach |
|------------|---------------------|
| `t.test()` | Use `@pandas_udf` with `scipy.stats.ttest_ind` |
| `lm()` / `glm()` | Use `sklearn` or `mlflow` for model fitting |
| `kmeans()` | Use `pyspark.ml.clustering.KMeans` |
| `chisq.test()` | Use `@pandas_udf` with `scipy.stats.chi2_contingency` |
| `shapiro.test()` | Use `@pandas_udf` with `scipy.stats.shapiro` |
| `cor.test()` | Use `@pandas_udf` with `scipy.stats.pearsonr` |

---

## Type System Mapping

### R types → PySpark types

| R Type | PySpark Type | Notes |
|--------|-------------|-------|
| `numeric` / `double` | `DoubleType()` | |
| `integer` | `IntegerType()` | R integers are 32-bit |
| `character` | `StringType()` | |
| `logical` | `BooleanType()` | `TRUE`/`FALSE` → `True`/`False` |
| `Date` | `DateType()` | |
| `POSIXct` / `POSIXlt` | `TimestampType()` | |
| `factor` | `StringType()` | No factor type in PySpark; use string + ordering |
| `complex` | Not supported | Decompose to real/imaginary DoubleType columns |
| `raw` | `BinaryType()` | |
| `list` (column) | `ArrayType()` or `StructType()` | Depends on content |
| `data.frame` (nested) | `StructType()` | |

### Type casting

**R:**
```r
df %>% mutate(
  amount = as.numeric(amount_str),
  id = as.integer(id_str),
  is_active = as.logical(active_flag),
  order_date = as.Date(date_str, format = "%Y-%m-%d")
)
```

**PySpark:**
```python
df = (
    df
    .withColumn("amount", F.col("amount_str").cast("double"))
    .withColumn("id", F.col("id_str").cast("int"))
    .withColumn("is_active", F.col("active_flag").cast("boolean"))
    .withColumn("order_date", F.to_date("date_str", "yyyy-MM-dd"))
)
```

### Checking types

**R:**
```r
is.numeric(x)
is.character(x)
class(df$col)
str(df)
```

**PySpark:**
```python
# Check schema
df.printSchema()
df.dtypes  # Returns list of (name, type) tuples

# Check specific column type
df.schema["col"].dataType
```

---

## Conditional and Logical Functions

| R | PySpark | Notes |
|---|---------|-------|
| `ifelse(cond, yes, no)` | `F.when(cond, yes).otherwise(no)` | |
| `dplyr::if_else(cond, yes, no)` | `F.when(cond, yes).otherwise(no)` | |
| `dplyr::case_when(...)` | `F.when().when().otherwise()` | Chain `.when()` calls |
| `xor(a, b)` | `(a & ~b) \| (~a & b)` | No direct XOR |
| `any(x)` | Not column-level; use `.filter().count() > 0` | |
| `all(x)` | Not column-level; use `.filter(~cond).count() == 0` | |
| `which(x > 0)` | `.filter(F.col("x") > 0)` | Returns rows, not indices |
| `match(x, table)` | `.join()` or `.isin()` | |
| `%in%` | `.isin([...])` | |
| `between(x, 1, 10)` | `F.col("x").between(1, 10)` | |
