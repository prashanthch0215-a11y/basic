# PySpark Interview Prep — 25 Questions (Databricks / Azure / Alteryx-Migration JD)

## Datasets (upload to a Databricks Volume or read locally)
- `customers.csv` — 200 customers, has nulls in `email`
- `products.csv` — 50 products
- `orders.csv` — ~1,600+ orders. **Intentionally dirty**: duplicate rows, bad `quantity` (-1, nulls), bad `order_date` ("invalid_date"), and repeated `order_id`s with different `order_status`/`last_updated_ts` (simulating late-arriving updates)
- `returns_flatfile.json` — nested JSON, one object per line (JSON Lines), simulates a messy flat-file drop with a nested `order_ref` struct and an `items` array

Load pattern to start every exercise with:
```python
orders = spark.read.option("header", True).csv("/path/orders.csv")
customers = spark.read.option("header", True).csv("/path/customers.csv")
products = spark.read.option("header", True).csv("/path/products.csv")
returns = spark.read.json("/path/returns_flatfile.json")
```

---

## Section A — Core Transformations (Alteryx tool equivalents)

**1.** `orders.csv` has exact duplicate rows (simulating a re-dropped file). Write PySpark to identify and remove **only true full-row duplicates**, and separately count how many were removed.

**2.** Some `order_id`s appear multiple times with *different* `order_status` and `last_updated_ts` (late-arriving updates — like Alteryx's "keep last record"). Write code to keep only the **latest version per `order_id`** based on `last_updated_ts`.

**3.** `quantity` contains nulls and invalid values (`-1`). Replace nulls with `1` (business default) and flag rows with `quantity <= 0` into a separate "rejected" DataFrame instead of silently dropping them.

**4.** `order_date` contains some literal string `"invalid_date"`. Parse `order_date` to a proper `DateType`, and route unparseable rows to a `_rescued_data`-style quarantine DataFrame rather than crashing the job.

**5.** Replicate an Alteryx "Summarize" tool: produce total revenue (`quantity * unit_price`), order count, and average order value **per customer per month**.

**6.** Replicate Alteryx "Cross Tab" (pivot): produce a DataFrame with one row per `customer_id` and one column per `order_status` showing count of orders in that status.

**7.** Replicate Alteryx "Transpose" (unpivot): take the pivoted result from Q6 and unpivot it back to long format (`customer_id`, `order_status`, `count`).

**8.** Join `orders` to `customers` and `products`. Produce a denormalized "Gold" table with `customer_name`, `region`, `product_name`, `category`, `revenue`. Handle the case where a `product_id` in orders doesn't exist in `products` (orphaned foreign key) — don't silently inner-join it away without logging it.

## Section B — Window Functions (Alteryx "Multi-Row", "Running Total", "Unique" equivalents)

**9.** For each customer, rank their orders by `order_date` and return only the **first order** (customer's earliest order = "new customer acquisition date").

**10.** Calculate a **running total of revenue per customer**, ordered by `order_date` (Alteryx "Running Total" tool equivalent).

**11.** For each customer, calculate the **days between consecutive orders** (`lag()`), and flag customers where the gap exceeds 90 days as "at risk of churn."

**12.** Deduplicate `orders` keeping only the row with the **highest `last_updated_ts` per `order_id`**, using a window function instead of `groupBy` (compare performance/readability trade-offs in your answer).

**13.** Rank products **within each category** by total revenue and return only the **top 2 products per category** (classic "top-N per group" — very common interview question).

## Section C — Semi-structured / Nested Data (flat-file / JSON handling)

**14.** `returns_flatfile.json` has a nested struct `order_ref` (with `order_id`, `customer_id`) and an array field `items`. Flatten this into a fully tabular DataFrame — explode `items` so each row is one returned SKU, and pull `order_id`/`customer_id` out of the nested struct into top-level columns.

**15.** Some `reason` values in the returns file are `null`. Write logic to categorize nulls as `"UNSPECIFIED"` and produce a count of return reasons by category.

**16.** Join the flattened `returns` data back to `orders` to calculate a **return rate per product** (returns / total orders for that product).

## Section D — Delta Lake / Merge / Upsert

**17.** Write the cleaned `orders` DataFrame as a **Delta table** partitioned by `order_status`. Then write a `MERGE INTO` statement that upserts new incoming order records — updating `order_status` if the `order_id` already exists, inserting if it doesn't.

**18.** Explain (and write) how you would use Delta **time travel** to compare today's `orders` gold table against **yesterday's version** to identify which rows changed (a reconciliation/audit use case, tying back to legacy-vs-new validation).

**19.** Your silver table has a **small-file problem** after months of daily incremental Autoloader writes. Write the `OPTIMIZE ... ZORDER BY` command you'd run, and explain which column(s) you'd Z-order on for this dataset and why.

## Section E — Autoloader / Ingestion

**20.** Write an Autoloader (`cloudFiles`) read for the `returns_flatfile.json`-style data landing incrementally in ADLS, with schema evolution enabled (`addNewColumns`), a `schemaLocation`, and `trigger(availableNow=True)`. Add `_ingestion_timestamp` and `_source_file` columns.

**21.** A new field silently starts appearing in the returns JSON files next month (e.g., `warehouse_id`). Explain what happens under `schemaEvolutionMode = "addNewColumns"` vs `"rescue"` vs `"none"`, and which you'd choose for a production pipeline and why.

## Section F — Data Quality & Validation (legacy vs. new parity)

**22.** Write a reusable PySpark function `reconcile(df_legacy, df_new, key_cols)` that returns three DataFrames: rows only in legacy (missing after migration), rows only in new (unexpected extras), and rows in both with **value mismatches** on shared columns.

**23.** Using `orders` and `returns`, compute an aggregate checksum comparison (sum of revenue, count distinct `customer_id`, min/max `order_date`) as a quick "did the migration blow up the data" sanity check — write it as one function you could run against any two DataFrames.

## Section G — Performance & Debugging

**24.** Your join between `orders` (1,600 rows) and `products` (50 rows) is slow in a scenario where `orders` is actually 500M rows in production. What would you check, and rewrite the join using a **broadcast hint**. Explain how you'd confirm via `.explain()` that the broadcast actually happened.

**25.** A silver-layer job that used to take 5 minutes now takes 40 minutes with no data volume change. Walk through your debugging checklist (Spark UI stages, shuffle read/write size, skew, small files, cluster autoscaling behavior, `explain()` plan changes) — no code needed, just your diagnostic process.

---

## How to use this
- Do Section A + B first — these map most directly to "convert Alteryx logic to PySpark," the core of the JD.
- Section C is your flat-file/semi-structured ingestion practice.
- Section D + E are Delta/Autoloader — be ready to write these from memory.
- Section F is what makes you sound senior — validation discipline is explicitly called out in the JD ("validate output to ensure high parity with legacy systems").
- Section G has no "right code" — it's about whether you can talk through performance debugging like someone who's actually run production jobs.
