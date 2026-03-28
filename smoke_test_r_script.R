#!/usr/bin/env Rscript
# =============================================================================
# R-to-PySpark Smoke Test Script
# =============================================================================
# This script exercises a wide range of R patterns that should be converted
# to idiomatic PySpark for Databricks. It uses tables in:
#   jss_sandbox_catalog.r_to_pyspark_smoke_test (orders, customers, products)
# =============================================================================

library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)
library(ggplot2)

# --- Configuration ---
catalog <- "jss_sandbox_catalog"
schema <- "r_to_pyspark_smoke_test"

# --- 1. Read data ---
orders    <- read.csv("orders.csv")
customers <- read.csv("customers.csv")
products  <- read.csv("products.csv")

# --- 2. Basic filtering and column creation ---
completed_orders <- orders %>%
  filter(status == "completed") %>%
  mutate(
    revenue      = quantity * unit_price,
    order_month  = floor_date(as.Date(order_date), "month"),
    order_year   = year(as.Date(order_date)),
    days_ago     = as.numeric(difftime(Sys.Date(), as.Date(order_date), units = "days"))
  )

# --- 3. String operations on customer data ---
customers_clean <- customers %>%
  mutate(
    name_upper     = toupper(name),
    name_title     = str_to_title(name),
    email_domain   = str_extract(email, "@(.+)$"),
    name_length    = nchar(name),
    has_gmail      = str_detect(email, "gmail"),
    customer_code  = str_pad(customer_id, 6, "left", "0")
  )

# --- 4. Multi-table join ---
full_data <- completed_orders %>%
  left_join(customers, by = "customer_id") %>%
  left_join(products, by = "product_id") %>%
  mutate(
    margin       = revenue - (quantity * cost),
    margin_pct   = margin / revenue * 100
  )

# --- 5. Grouped aggregation with multiple summaries ---
region_summary <- full_data %>%
  group_by(region.y, category) %>%
  summarize(
    total_revenue     = sum(revenue, na.rm = TRUE),
    total_margin      = sum(margin, na.rm = TRUE),
    avg_margin_pct    = mean(margin_pct, na.rm = TRUE),
    order_count       = n(),
    unique_customers  = n_distinct(customer_id),
    max_single_order  = max(revenue, na.rm = TRUE),
    min_single_order  = min(revenue, na.rm = TRUE),
    first_order_date  = min(order_date),
    last_order_date   = max(order_date),
    .groups = "drop"
  ) %>%
  arrange(desc(total_revenue))

# --- 6. Case-when / conditional logic ---
tiered_orders <- completed_orders %>%
  mutate(
    value_tier = case_when(
      revenue >= 500   ~ "premium",
      revenue >= 100   ~ "standard",
      revenue >= 25    ~ "economy",
      TRUE             ~ "micro"
    ),
    size_category = case_when(
      quantity >= 5  ~ "bulk",
      quantity >= 3  ~ "multi",
      quantity == 1  ~ "single",
      TRUE           ~ "pair"
    ),
    is_high_value = ifelse(revenue > 100, TRUE, FALSE)
  )

# --- 7. Window functions ---
customer_order_history <- completed_orders %>%
  group_by(customer_id) %>%
  arrange(order_date) %>%
  mutate(
    order_rank       = row_number(),
    total_orders     = n(),
    prev_revenue     = lag(revenue, 1),
    next_revenue     = lead(revenue, 1),
    running_total    = cumsum(revenue),
    running_avg      = cummean(revenue),
    pct_of_customer  = revenue / sum(revenue) * 100
  ) %>%
  ungroup()

# --- 8. Top-N per group ---
top_orders_per_region <- completed_orders %>%
  group_by(region) %>%
  slice_max(order_by = revenue, n = 2) %>%
  ungroup()

# --- 9. Pivot wider (long to wide) ---
revenue_by_region_month <- completed_orders %>%
  mutate(month_str = format(as.Date(order_date), "%Y-%m")) %>%
  group_by(region, month_str) %>%
  summarize(total_revenue = sum(revenue), .groups = "drop")

wide_revenue <- revenue_by_region_month %>%
  pivot_wider(
    names_from   = month_str,
    values_from  = total_revenue,
    values_fill  = 0
  )

# --- 10. Statistical summaries ---
overall_stats <- completed_orders %>%
  summarize(
    avg_revenue       = mean(revenue),
    median_revenue    = median(revenue),
    sd_revenue        = sd(revenue),
    q25               = quantile(revenue, 0.25),
    q75               = quantile(revenue, 0.75),
    total_orders      = n(),
    total_revenue     = sum(revenue)
  )

# --- 11. Date operations with lubridate ---
date_analysis <- completed_orders %>%
  mutate(
    day_of_week   = wday(as.Date(order_date), label = TRUE),
    week_of_year  = week(as.Date(order_date)),
    quarter       = quarter(as.Date(order_date)),
    next_month    = as.Date(order_date) + months(1),
    formatted     = format(as.Date(order_date), "%B %d, %Y")
  )

# --- 12. Complex chained pipeline ---
final_report <- orders %>%
  filter(status != "cancelled") %>%
  left_join(products, by = "product_id") %>%
  mutate(
    revenue   = quantity * unit_price.x,
    margin    = revenue - (quantity * cost),
    month     = floor_date(as.Date(order_date), "month")
  ) %>%
  group_by(month, category) %>%
  summarize(
    total_revenue   = sum(revenue),
    total_margin    = sum(margin),
    avg_order_value = mean(revenue),
    order_count     = n(),
    .groups = "drop"
  ) %>%
  mutate(
    margin_rate      = total_margin / total_revenue * 100,
    revenue_rank     = dense_rank(desc(total_revenue))
  ) %>%
  arrange(month, revenue_rank)

# --- 13. Visualization ---
monthly_chart_data <- completed_orders %>%
  mutate(month = floor_date(as.Date(order_date), "month")) %>%
  group_by(month, region) %>%
  summarize(revenue = sum(revenue), .groups = "drop")

p <- ggplot(monthly_chart_data, aes(x = month, y = revenue, fill = region)) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(
    title = "Monthly Revenue by Region",
    x     = "Month",
    y     = "Revenue ($)"
  ) +
  theme_minimal()

print(p)

# --- 14. Write outputs ---
write.csv(region_summary, "output/region_summary.csv", row.names = FALSE)
write.csv(final_report, "output/final_report.csv", row.names = FALSE)
write.csv(customer_order_history, "output/customer_order_history.csv", row.names = FALSE)

cat("Script complete. Generated 3 output files.\n")
