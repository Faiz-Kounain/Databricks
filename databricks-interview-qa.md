# Databricks Interview Questions and Answers (30 Questions)

Simple-English answers for mid and senior Data Engineer interviews.
Each question has: **simple idea**, a small **analogy**, a tiny **example** (where useful), and a **🎤 Say this in the interview** answer you can speak out loud.

> **Your project story (use it everywhere):** SAS to PySpark migration on Databricks on AWS. Client Parquet files sit in S3. We read them through a **Unity Catalog external location**, load them into **bronze Delta tables**, then clean and join them into **silver** and **gold**. Jobs run as **Databricks Workflows** (about 14 tasks in a line, with an `env=prod` parameter) on **autoscaling r6g job clusters (2 to 10 workers)**. We check results with the **team's Python recon utility** (two DataFrames + primary key, gives an HTML report). Oracle data is read with **Lakehouse Federation over PrivateLink**. Code is deployed with **Asset Bundles + GitHub Actions**.
>
> Only say what is true. If you only learned something in practice (like SCD2 or Auto Loader), say "I have done this hands-on in practice" instead of claiming it in the client project.

### 💡 Why this document matters

In the Flutter interview you were asked "which Databricks features did you use?" and you only said time travel. Next time, say this list with confidence:

> 🎤 "In my project I used **Delta Lake** tables in a **bronze, silver, gold** design, **Unity Catalog** with **external locations** to read client Parquet from S3, **Databricks Workflows** with task parameters like `env=prod`, **autoscaling job clusters**, **MERGE** for upserts, **time travel** for debugging and restores, **OPTIMIZE** and **VACUUM** for maintenance, **Lakehouse Federation** to read Oracle, and **Asset Bundles with GitHub Actions** for CI/CD."

That is about 10 features in one breath. Practise it 5 times.

---

# 🟢 Round 1: Mid level (core)

## Q1. What is a Lakehouse? How is it different from a data lake or a warehouse?

**Simple idea**
- A **data warehouse** (like Snowflake, Redshift) stores clean tables. Fast SQL, good for reports. But costly and not great for raw files, images, or ML.
- A **data lake** (like S3) stores any file cheaply. But it has no rules: no transactions, no updates, files can get messy ("data swamp").
- A **Lakehouse** = cheap storage of a lake **+** the rules and speed of a warehouse. On Databricks, this is done with **Delta Lake** files on S3 plus **Unity Catalog** for security.

**Analogy**
- Lake = a big storeroom where you throw everything.
- Warehouse = a neat shop with price tags, but expensive rent.
- Lakehouse = the same cheap storeroom, but now with shelves, labels, and a register book. So it works like a shop.

| | Data lake | Warehouse | Lakehouse |
|---|---|---|---|
| Storage cost | Low | High | Low |
| ACID (safe updates) | No | Yes | Yes (Delta) |
| Raw + ML data | Yes | Weak | Yes |
| BI / SQL speed | Weak | Strong | Strong |

**🎤 Say this in the interview**
"A lakehouse keeps data in open files on cheap object storage like S3, but adds warehouse features on top: ACID transactions, schema enforcement, time travel and governance. On Databricks that is Delta Lake plus Unity Catalog. In my project, the client's Parquet in S3 became bronze Delta tables, and we built silver and gold on the same storage, so BI and data engineering used one copy of data instead of a separate lake and warehouse."

---

## Q2. What is Delta Lake? What does the `_delta_log` do?

**Simple idea**
Delta Lake = **Parquet files + a transaction log**. The log is the folder `_delta_log`. Every change (insert, update, delete, merge) writes a new small JSON file there: `000000.json`, `000001.json`, ... Each file says "these Parquet files were added, these were removed".

When Spark reads a Delta table, it first reads the log to know **which Parquet files are the current version**.

Every 10 commits (default), Delta writes a **checkpoint** file (Parquet) so it does not need to read thousands of JSON files.

**Analogy**
The Parquet files are pages of a book. `_delta_log` is the **table of contents** that says which pages are valid right now. If a page is torn out (deleted), the contents page just stops listing it.

**What the log gives you**
- **ACID transactions**: a write either fully happens or not at all. Readers never see half-written data.
- **Time travel**: old versions are still listed in the log.
- **Schema enforcement**: wrong columns are rejected.
- **Concurrent writes**: optimistic concurrency, two writers are checked against the log.

**Tiny example**
```sql
DESCRIBE HISTORY silver.customers;   -- shows each version from _delta_log
DESCRIBE DETAIL silver.customers;    -- location, number of files, size
```

**🎤 Say this in the interview**
"Delta Lake is an open table format: data is stored as Parquet, and a transaction log in `_delta_log` records every commit as JSON, with Parquet checkpoints every few commits. Readers use the log to find the current set of files, which gives ACID transactions, schema enforcement, and time travel. In my project, all our bronze, silver and gold tables were Delta, so a failed task never left a half-written table."

---

## Q3. All-purpose vs job compute vs serverless: when do you use each?

**Simple idea**
- **All-purpose cluster**: you start it, many people attach notebooks, it stays on until auto-terminate. Good for **development and exploring**. Costs more per DBU.
- **Job cluster**: created when a Workflow job starts, **deleted when the job ends**. Good for **scheduled production jobs**. Cheaper DBU rate and clean every run.
- **Serverless**: Databricks manages the machines. Starts in seconds, no cluster config. Good for SQL warehouses, quick jobs, notebooks, and when you do not want to tune clusters. You pay only while it runs.

**Analogy**
- All-purpose = your own car parked outside the office, always ready, but you pay parking all day.
- Job cluster = a taxi booked for one trip, gone after the trip.
- Serverless = Uber that arrives in 10 seconds and you never think about the car.

**🎤 Say this in the interview**
"I use all-purpose clusters only for development, with auto-terminate. Production runs on job clusters because they are cheaper, isolated, and destroyed after the run. In my project, our Workflow ran on an autoscaling r6g job cluster with 2 to 10 workers. Serverless is a good choice for SQL warehouses and short jobs where fast start-up matters more than fine control. I also practised on Databricks Free Edition, which is fully serverless."

---

## Q4. Bronze, silver and gold: what goes in each layer in your project?

**Simple idea** (this is called the **medallion architecture**)
- **Bronze** = raw data, almost as it came. Add only audit columns like `load_date`, `source_file`. Never lose data here.
- **Silver** = cleaned data: correct data types, nulls handled, duplicates removed, tables joined, business keys fixed.
- **Gold** = business-ready tables: aggregates, KPIs, reporting tables. What the analysts and dashboards use.

**Analogy**
Bronze = vegetables from the market (with mud). Silver = washed and cut vegetables. Gold = the cooked dish on the plate.

**Tiny example**
```python
# bronze: read client parquet from S3 (through UC external location)
raw = spark.read.parquet("s3://client-bucket/sales/2026-11-01/")
(raw.withColumn("load_ts", F.current_timestamp())
    .write.mode("append").saveAsTable("bronze.sales"))

# silver: clean
silver = (spark.table("bronze.sales")
          .dropDuplicates(["order_id"])
          .filter("order_id IS NOT NULL")
          .withColumn("amount", F.col("amount").cast("decimal(18,2)")))
silver.write.mode("overwrite").saveAsTable("silver.sales")

# gold: business table
(spark.table("silver.sales").groupBy("campaign_id")
      .agg(F.sum("amount").alias("revenue"))
      .write.mode("overwrite").saveAsTable("gold.campaign_revenue"))
```

**🎤 Say this in the interview**
"In my project, bronze held the client's Parquet files from S3 loaded as-is into Delta, with load timestamps. Silver was where we put the converted SAS logic: type casting, filtering, de-duplication, and joins. Gold had the final reporting tables that matched the old SAS outputs. Keeping bronze raw meant we could always rebuild silver and gold without asking the client to resend files."

---

## Q5. How do you do an upsert in Delta? (MERGE)

**Simple idea**
**Upsert** = update the row if it exists, insert it if it is new. In Delta we use `MERGE INTO`. It matches rows on a key.

**Analogy**
Like updating your phone contacts from a new list: if the name already exists, update the number. If not, add a new contact.

**Tiny example (SQL)**
```sql
MERGE INTO silver.customers AS t
USING staging_customers AS s
  ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

**Same in PySpark**
```python
from delta.tables import DeltaTable
t = DeltaTable.forName(spark, "silver.customers")
(t.alias("t")
  .merge(staging_df.alias("s"), "t.customer_id = s.customer_id")
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute())
```

**Important tips**
- The source must have **only one row per key**, otherwise MERGE fails ("multiple source rows matched"). De-duplicate first with `ROW_NUMBER()`.
- Add a condition to update only changed rows: `WHEN MATCHED AND t.hash <> s.hash THEN UPDATE ...`
- `WHEN NOT MATCHED BY SOURCE THEN DELETE` can remove rows missing from the source.
- MERGE is **idempotent**: running it twice with the same data gives the same result.

**🎤 Say this in the interview**
"I use `MERGE INTO` with the business key in the ON clause: update when matched, insert when not matched. Before merging I de-duplicate the source with ROW_NUMBER so there is one row per key, otherwise MERGE throws an error. I also add a condition so only changed rows get updated, which reduces rewritten files. MERGE is also why our reruns were safe: running the same batch twice did not create duplicates."

---

## Q6. Time travel: how does it work, and how do you restore a table?

**Simple idea**
Because `_delta_log` keeps a list of every version, you can **read an older version** of the table. Old Parquet files stay on disk until `VACUUM` deletes them (default keeps 7 days).

**Analogy**
Like "Version history" in Google Docs: you can open yesterday's version or restore it.

**Tiny example**
```sql
DESCRIBE HISTORY gold.campaign_revenue;          -- see versions

SELECT * FROM gold.campaign_revenue VERSION AS OF 12;
SELECT * FROM gold.campaign_revenue TIMESTAMP AS OF '2026-11-01';

RESTORE TABLE gold.campaign_revenue TO VERSION AS OF 12;   -- undo a bad load
```
```python
spark.read.option("versionAsOf", 12).table("gold.campaign_revenue")
```

**Limits**
- After `VACUUM`, old files are gone, so you cannot go back further than the retention period.
- `RESTORE` itself is a new version, so you can even undo a restore.

**Uses**: undo a bad load, debug "what changed since yesterday", audit, reproduce an old report.

**🎤 Say this in the interview**
"Time travel works because the Delta log keeps every version. I can query `VERSION AS OF` or `TIMESTAMP AS OF`, and use `RESTORE TABLE` to roll back after a bad load. In my project I used it to compare a table before and after a run when numbers did not match SAS, and to check what a task had changed. It only goes back as far as VACUUM retention, which is 7 days by default."

---

## Q7. OPTIMIZE, Z-ORDER and VACUUM: what does each one do?

**Simple idea**
- **OPTIMIZE** = joins many **small files** into fewer **big files** (about 1 GB). Small files make reads slow.
- **Z-ORDER BY (col)** = while optimizing, it puts rows with similar values of that column **in the same files**. Then a filter like `WHERE customer_id = 5` can **skip** most files (data skipping).
- **VACUUM** = **deletes old files** that no table version needs any more (older than 7 days by default). Saves storage cost. But after it, time travel to those versions is not possible.

**Analogy**
- OPTIMIZE = putting 1,000 loose papers into 10 neat files.
- Z-ORDER = sorting those files by customer name, so you open only the right file.
- VACUUM = throwing away old drafts from the bin to free space.

**Tiny example**
```sql
OPTIMIZE silver.sales ZORDER BY (customer_id);
VACUUM silver.sales;                 -- default 7 days (168 hours)
VACUUM silver.sales RETAIN 240 HOURS;
```

**Tips**
- Z-ORDER on columns you **filter on a lot** and that have **many distinct values** (ids, dates). Not on low-cardinality columns like gender.
- Do not use `RETAIN 0 HOURS` in prod: it can break running readers and time travel.
- New tables: Databricks recommends **liquid clustering** instead of partitioning + Z-ORDER (see Q14). **Predictive optimization** in Unity Catalog can run OPTIMIZE/VACUUM automatically.

**🎤 Say this in the interview**
"OPTIMIZE compacts small files into larger ones, Z-ORDER co-locates rows by frequently filtered columns so Delta can skip files, and VACUUM removes files no longer referenced, after the retention period. Small files are a common problem with daily appends, so we ran OPTIMIZE as maintenance and VACUUM with the default 7-day retention so we still had time travel for debugging."

---

## Q8. What is Auto Loader? How is it different from `spark.read`?

**Simple idea**
- `spark.read.parquet(path)` reads **all files in the folder every time**. It does not remember what it read before. You must handle "which files are new" yourself.
- **Auto Loader** (`spark.readStream.format("cloudFiles")`) **remembers which files it already loaded** (in a checkpoint). Each run picks up **only new files**. It can also handle **schema changes**.

**Analogy**
`spark.read` = reading the whole newspaper pile every morning to find today's paper. Auto Loader = a helper who keeps a list and only hands you today's new paper.

**Tiny example**
```python
(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "parquet")
   .option("cloudFiles.schemaLocation", "/Volumes/main/meta/schemas/sales")
   .load("s3://client-bucket/sales/")
 .writeStream
   .option("checkpointLocation", "/Volumes/main/meta/checkpoints/sales")
   .trigger(availableNow=True)          # process new files, then stop (batch style)
   .toTable("bronze.sales"))
```

**Key points**
- Two modes to find new files: **directory listing** (default) or **file notification** (uses SQS/SNS on AWS, for millions of files).
- `trigger(availableNow=True)` lets you run it like a **daily batch job** but still incremental.
- Schema evolution with `cloudFiles.schemaEvolutionMode` and `_rescued_data` column (Q12).

**🎤 Say this in the interview**
"`spark.read` is a one-time read of everything at the path, so you need your own logic to avoid reprocessing. Auto Loader is incremental: it tracks processed files in a checkpoint, picks up only new ones, and supports schema inference and evolution. With `availableNow` it runs like a batch job. In my project we read the client Parquet with `spark.read` by date folder. For new pipelines I would use Auto Loader, and I have built one hands-on in practice."

---

## Q9. Managed vs external tables: what happens when you DROP each?

**Simple idea**
- **Managed table**: Databricks / Unity Catalog decides where the files live (managed storage). **DROP TABLE deletes the table and the data files** (in UC, files are removed after a short grace period, and you can `UNDROP` within 7 days).
- **External table**: **you** give the location (`LOCATION 's3://...'`). **DROP TABLE removes only the table definition**. The files stay in S3.

**Analogy**
Managed = renting a furnished flat; when you leave, the furniture goes too. External = you bring your own furniture; when you leave, you take it with you.

**Tiny example**
```sql
-- managed
CREATE TABLE silver.sales (id INT, amount DOUBLE);

-- external (needs an external location in Unity Catalog)
CREATE TABLE bronze.client_sales
LOCATION 's3://client-bucket/delta/sales/';
```

**When to use**
- Managed: default choice. UC can auto-optimize it (predictive optimization).
- External: data is shared with other tools/teams, or you must control the S3 path.

**🎤 Say this in the interview**
"Dropping a managed table deletes both metadata and data, while dropping an external table removes only the metadata and leaves files in storage. Managed tables are the recommended default because Unity Catalog fully governs and optimizes them. External tables make sense when the data is owned or shared outside Databricks. In my project, the client's raw Parquet stayed in their S3 bucket and we read it through a UC external location, while our own bronze, silver and gold Delta tables lived in our catalog."

---

## Q10. How do you pass parameters and values between Workflow tasks? (widgets, task values)

**Simple idea**
- **Job / task parameters**: values given when the job starts, like `env=prod` or `run_date=2026-11-01`. In a notebook you read them with **widgets**.
- **Task values**: one task **sets** a value while running, and a later task **gets** it. Good for small things like row counts, a file name, or a status.
- **Dynamic references**: in the job config you can use `{{job.parameters.env}}`, `{{tasks.task_a.values.row_count}}`, `{{job.start_time.iso_date}}`.

**Analogy**
Parameters = instructions written on the job ticket before work starts. Task values = a sticky note one worker leaves for the next worker.

**Tiny example**
```python
# Notebook in any task: read a parameter
env = dbutils.widgets.get("env")          # "prod"

# Task A: set a value
dbutils.jobs.taskValues.set(key="row_count", value=df.count())

# Task B: get the value from Task A
cnt = dbutils.jobs.taskValues.get(taskKey="task_a", key="row_count", default=0)
```

**Tips**
- Task values must be small (JSON, up to about 48 KB). Do not pass DataFrames; pass a **table name** instead.
- Use parameters to switch catalogs: `catalog = f"{env}_catalog"`.

**🎤 Say this in the interview**
"For inputs I use job parameters, read in notebooks with `dbutils.widgets.get`. In my project the Workflow had about 14 linear tasks and an `env=prod` parameter, which each notebook used to pick the right catalog and paths. To pass results between tasks, like a row count or a file name, I use `dbutils.jobs.taskValues.set` and `get`, or dynamic references like `{{tasks.x.values.y}}`. For large data, tasks share it through Delta tables, not task values."

---

# 🟡 Round 2: Senior level (design + performance)

## Q11. Unity Catalog: metastore, catalog/schema/table, external location, storage credential

**Simple idea**
Unity Catalog (UC) is the **one place for security and governance** of all data in Databricks.

- **Metastore** = the top box. Usually **one per region**. Workspaces attach to it.
- **3-level names**: `catalog.schema.table`, for example `prod.silver.sales`.
  - **Catalog** = big folder, often one per environment or business area (`dev`, `prod`).
  - **Schema** = sub-folder (`bronze`, `silver`, `gold`).
  - **Table / view / volume / function / model** = the actual objects.
- **Storage credential** = **how** Databricks logs into cloud storage. On AWS this is an **IAM role** that Databricks can assume.
- **External location** = **where**: an S3 path + the storage credential to use. You grant users access on it (`READ FILES`).

**Analogy**
Metastore = the whole building. Catalog = a floor. Schema = a room. Table = a cupboard. Storage credential = the key card. External location = "this key card opens this door (S3 path)".

**Tiny example**
```sql
CREATE STORAGE CREDENTIAL client_cred ...;           -- IAM role (usually via UI)
CREATE EXTERNAL LOCATION client_sales
  URL 's3://client-bucket/sales/'
  WITH (STORAGE CREDENTIAL client_cred);

GRANT READ FILES ON EXTERNAL LOCATION client_sales TO `data_engineers`;
GRANT USE CATALOG ON CATALOG prod TO `analysts`;
GRANT SELECT ON TABLE prod.gold.campaign_revenue TO `analysts`;
```

**Extra UC features to mention**: lineage (see which table feeds which), audit logs, column masks and row filters (Q27), Delta Sharing, volumes for files.

**🎤 Say this in the interview**
"Unity Catalog is a central governance layer. A metastore per region holds catalogs, schemas and tables, so every object has a three-level name. Access to cloud storage is through a storage credential, which on AWS is an IAM role, and an external location, which binds an S3 path to that credential. In my project, the client's Parquet was in S3 and we read it through a UC external location, so no access keys were in code and access was controlled with grants. UC also gave us lineage and audit."

---

## Q12. How do you handle schema evolution? (`mergeSchema`, Auto Loader rescue column)

**Simple idea**
Schema evolution = the source adds a new column (or changes a type), and your pipeline should not break.

By default Delta has **schema enforcement**: if you write a DataFrame with an extra column, it **fails**. That is good protection. To **allow** new columns:

1. **`mergeSchema`** on write: new columns are added to the table.
   ```python
   df.write.option("mergeSchema", "true").mode("append").saveAsTable("bronze.sales")
   ```
2. **MERGE with schema evolution**: `MERGE WITH SCHEMA EVOLUTION INTO ...` (or the setting `spark.databricks.delta.schema.autoMerge.enabled = true`).
3. **Auto Loader**:
   - `cloudFiles.schemaEvolutionMode = "addNewColumns"` (default): when a new column appears, the stream stops once, updates the schema, and works after restart.
   - `rescue`: never changes schema; unexpected data goes into the **`_rescued_data`** column as JSON.
   - Type mismatch values also go to `_rescued_data`, so nothing is lost.
4. **Type changes** (like INT to STRING) are harder: often you need `overwriteSchema` or a rebuild. Newer Delta supports **type widening** (INT to BIGINT).

**Analogy**
Schema enforcement = a bouncer who only lets people on the guest list in. mergeSchema = adding new names to the guest list. Rescue column = a "lost and found" box for things that do not fit.

**Good practice**
- Bronze: be **flexible** (accept new columns, rescue column).
- Silver/Gold: be **strict** (fixed schema, tests), so dashboards do not break silently.
- Alert when the schema changes.

**🎤 Say this in the interview**
"Delta enforces schema by default, which I like, because it stops bad data. Where I want to allow new columns, I use `mergeSchema` on append or schema evolution in MERGE. With Auto Loader I use `addNewColumns` mode, and the `_rescued_data` column catches values that do not match, so nothing is silently lost. My rule is flexible in bronze, strict in silver and gold, with an alert when the source schema changes."

---

## Q13. A job suddenly got slow. How do you debug it step by step?

**Simple idea**: do not guess. Look, then fix.

**Step by step**
1. **Compare with a good run**: same code? More data? Different cluster? Check job run history and input row counts.
2. **Open the Spark UI** for the slow run:
   - **Jobs / Stages tab**: find the **stage that takes the most time**.
   - **Task metrics** in that stage: compare **max vs median** task time. If max is 10x median, that is **data skew** (one key has too much data).
   - **Spill (memory/disk)**: if large, executors do not have enough memory for the partitions.
   - **Shuffle read/write**: very large shuffle means a big join or groupBy.
   - **SQL / DataFrame tab**: see the plan, check join type (SortMerge vs Broadcast) and whether filters were pushed down.
3. **Check the input**: many **small files**? Missing **file skipping** (no Z-ORDER/clustering)? Reading all partitions instead of one?
4. **Check the cluster**: Ganglia / metrics: CPU low? Memory full? Autoscaling stuck at min? Spot instances lost?

**Common fixes**
| Problem | Fix |
|---|---|
| Skew | Enable AQE skew join, **salting** the hot key, broadcast the small side |
| Spill | More memory per core, more shuffle partitions, memory-optimised nodes (r-series) |
| Small files | OPTIMIZE, auto compaction, fewer output partitions |
| Big shuffle | Broadcast join for small tables, filter early, select only needed columns |
| Full scan | Partition pruning, liquid clustering, Z-ORDER on filter columns |
| Python UDF slow | Replace with built-in functions |
| Recomputing same DF | `cache()` if reused several times |

**Analogy**
Like a doctor: first check temperature and tests (Spark UI), then give medicine (fix). Not medicine first.

**🎤 Say this in the interview**
"First I compare with the last good run: data volume, code changes, cluster. Then in the Spark UI I find the slowest stage and look at task time distribution. If the max task is far above the median, it is skew, and I use AQE skew handling, salting, or a broadcast join. If I see spill, I increase memory or shuffle partitions. I also check for small files and missing file skipping, and fix with OPTIMIZE or clustering. I change one thing at a time and measure."

---

## Q14. Liquid clustering vs partitioning vs Z-ORDER: when would you choose each?

**Simple idea**
All three help Spark **skip files** it does not need.

- **Partitioning** (`PARTITIONED BY (date)`): data is split into **folders** per value. Very fast for filters on that column. But: if the column has many values (like customer_id), you get **millions of tiny folders** (small files problem). And you cannot change it later without rewriting.
- **Z-ORDER**: sorts data inside files by 1 to 4 columns, during OPTIMIZE. Not incremental: OPTIMIZE ZORDER rewrites a lot each time.
- **Liquid clustering** (`CLUSTER BY (col)`): the **new recommended** way. Databricks clusters data automatically and incrementally. You can **change the cluster keys later** without rewriting all data. Works with high-cardinality columns. Cannot be combined with partitioning or Z-ORDER on the same table.

**Analogy**
- Partitioning = separate cupboards for each month. Great if you always ask "give me March".
- Z-ORDER = sorting clothes inside each cupboard, but you must re-sort everything each time.
- Liquid clustering = a smart cupboard that keeps itself sorted as you add clothes, and you can change the sort rule anytime.

**Tiny example**
```sql
CREATE TABLE gold.events (event_id BIGINT, user_id BIGINT, event_date DATE, ...)
CLUSTER BY (event_date, user_id);

ALTER TABLE gold.events CLUSTER BY (user_id);   -- change keys later
OPTIMIZE gold.events;                           -- clusters incrementally
-- or CLUSTER BY AUTO: Databricks picks keys from query patterns
```

**When to choose**
| Use | When |
|---|---|
| Liquid clustering | **Default for new tables**, query patterns may change, high-cardinality filter columns, concurrent writes |
| Partitioning | Very large tables (TBs) with a low-cardinality column always used in filters (like date), each partition at least ~1 GB, or older runtimes |
| Z-ORDER | Existing partitioned tables where you cannot migrate yet |

**🎤 Say this in the interview**
"For new Delta tables I choose liquid clustering, because it is incremental, works with high-cardinality columns, and I can change the clustering keys without rewriting the table. Partitioning only makes sense for very large tables with a low-cardinality column that is always filtered, like date, and with at least a gigabyte per partition. Z-ORDER is the older approach I would keep on existing partitioned tables. Over-partitioning is a common mistake because it creates small files."

---

## Q15. How do you implement SCD Type 2 in Delta?

**Simple idea**
**SCD Type 2** = keep **history**. When a customer's city changes, do not overwrite. **Close the old row** and **insert a new row**.

Columns used: `start_date`, `end_date`, `is_current`.

| customer_id | city | start_date | end_date | is_current |
|---|---|---|---|---|
| 101 | Pune | 2025-01-01 | 2026-03-10 | false |
| 101 | Hyderabad | 2026-03-10 | 9999-12-31 | true |

**Analogy**
Like your passport history: the old passport is marked "expired", a new one is issued. Both are kept.

**Tiny example (the "merge key trick")**
```sql
MERGE INTO gold.dim_customer t
USING (
  -- rows to update/insert normally
  SELECT s.customer_id AS merge_key, s.* FROM updates s
  UNION ALL
  -- changed rows again with NULL key, so they get INSERTED as new versions
  SELECT NULL AS merge_key, s.* FROM updates s
  JOIN gold.dim_customer t
    ON s.customer_id = t.customer_id AND t.is_current = true
  WHERE s.city <> t.city
) src
ON t.customer_id = src.merge_key AND t.is_current = true
WHEN MATCHED AND t.city <> src.city THEN
  UPDATE SET is_current = false, end_date = current_date()
WHEN NOT MATCHED THEN
  INSERT (customer_id, city, start_date, end_date, is_current)
  VALUES (src.customer_id, src.city, current_date(), '9999-12-31', true);
```

**Easier options**
- **DLT / Lakeflow**: `APPLY CHANGES INTO ... STORED AS SCD TYPE 2` (or `create_auto_cdc_flow`) does it for you.
- **dbt snapshots** (you did this on Snowflake).
- Compare many columns with a **hash** (`md5(concat_ws('|', ...))`) instead of `city <> city`.

**🎤 Say this in the interview** (honest version)
"SCD Type 2 keeps history by closing the current row and inserting a new version with start date, end date and an is-current flag. In Delta I do it with a single MERGE using the merge-key trick: changed rows appear twice in the source, once to expire the old row and once with a null key so they get inserted. I compare a hash of the tracked columns to detect changes. With DLT I can use APPLY CHANGES with SCD Type 2. I have built SCD2 hands-on in a banking data-model practice project and with dbt snapshots."

---

## Q16. Change Data Feed: what is it, and when do you use it over a full reload?

**Simple idea**
**Change Data Feed (CDF)** makes a Delta table **record which rows changed** (insert, update, delete) in each version. Then the next layer can read **only the changes**, not the full table.

```sql
ALTER TABLE silver.customers SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

SELECT * FROM table_changes('silver.customers', 15);   -- changes since version 15
```
Each row has extra columns: `_change_type` (`insert`, `delete`, `update_preimage`, `update_postimage`), `_commit_version`, `_commit_timestamp`.

```python
(spark.read.option("readChangeFeed", "true")
      .option("startingVersion", 15)
      .table("silver.customers"))
```

**Analogy**
Full reload = re-reading the whole book every day to find what changed. CDF = reading only the "edits" list at the back.

**When to use CDF**
- Table is big but **only a small % changes** each day.
- Silver has **updates/deletes** (MERGE), and gold must follow them.
- Sending changes to another system (downstream sync, audit).

**When a full reload is fine**
- Table is small.
- Most rows change every time anyway.
- Logic is complex and full recompute is simpler and cheap enough.

**🎤 Say this in the interview**
"Change Data Feed records row-level inserts, updates and deletes for each Delta version, which I can read with `table_changes` or `readChangeFeed` from a starting version. I use it when a large silver table gets merged and only a small part changes, so gold can process just those rows instead of a full reload. I store the last processed version so the next run starts from there. For small tables, a full overwrite is simpler and I would keep that."

---

## Q17. What is Photon, and what is AQE (Adaptive Query Execution)?

**Photon (simple idea)**
Photon is Databricks' **faster engine written in C++** (vectorised). It runs SQL and DataFrame operations faster than normal Spark (JVM). You just **turn it on** in the cluster; no code change. Best for SQL-heavy work: joins, aggregations, MERGE, writing Delta. It costs more DBUs per hour, but often finishes much faster, so total cost can be lower. Python UDFs do not get the speed-up.

Analogy: same car, but a turbo engine.

**AQE (simple idea)**
**Adaptive Query Execution** = Spark **changes the plan during the run**, using real statistics after each shuffle. It is **on by default** in Spark 3 / Databricks. It does 3 main things:
1. **Coalesce shuffle partitions**: joins too many tiny partitions into fewer.
2. **Switch join strategy**: if one side turns out small, change sort-merge join to **broadcast join**.
3. **Skew join handling**: splits a very big (skewed) partition into smaller pieces.

Analogy: Google Maps re-routing you while driving when it sees traffic.

```python
spark.conf.get("spark.sql.adaptive.enabled")                 # true
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

**🎤 Say this in the interview**
"Photon is Databricks' native vectorised engine in C++. It speeds up SQL and DataFrame work like joins, aggregations and Delta writes without code changes; it does not help Python UDFs. AQE is Spark's runtime re-optimisation: after each shuffle it uses real statistics to coalesce small partitions, switch to broadcast joins, and split skewed partitions. Both are on by default on most modern Databricks compute, so my first step is making sure we do not disable them and that we avoid UDFs that block Photon."

---

## Q18. How do you size a cluster for a 1 TB job?

**Simple idea**: work out **how many partitions**, then **how many cores**, then **memory**.

**Step by step (rule of thumb)**
1. **Partitions**: aim for ~128 MB per partition. 1 TB = 1,000,000 MB / 128 ≈ **8,000 partitions** (tasks).
2. **Cores**: you do not need 8,000 cores. Tasks run in **waves**. With **200 cores**, 8,000 tasks = 40 waves. If one task takes ~30 sec, that is ~20 minutes. More cores = fewer waves = faster (but costs more).
3. **Workers**: e.g. `r6g.4xlarge` (16 cores, 128 GB). 200 cores / 16 ≈ **12 to 13 workers**.
4. **Memory**: ~8 GB per core is comfortable for joins/shuffle. Use memory-optimised **r-series** for heavy joins and aggregations; compute-optimised **c-series** for simple CPU work.
5. **Shuffle partitions**: set `spark.sql.shuffle.partitions` close to the data size after filters (or leave AQE to coalesce from a higher number).
6. **Autoscaling**: set min and max (e.g. 4 to 16) so it grows only when needed.
7. **Test and tune**: run once, look at Spark UI (spill, idle cores, skew), then adjust.

**Analogy**
1 TB = 8,000 boxes to move. Each worker (person) has 16 hands (cores). More people = fewer trips, but you pay each person.

**Tip**: before adding machines, **reduce the data**: read only needed columns (Parquet is columnar), filter early, use file skipping. A 1 TB table may only need 100 GB read.

**🎤 Say this in the interview**
"I start from data size: 1 TB at about 128 MB per partition is around 8,000 tasks. Then I pick cores based on the run-time target; for example, 200 cores means about 40 waves. For join-heavy work I use memory-optimised nodes, so about a dozen r6g.4xlarge workers, with autoscaling and Photon on. I first reduce what is read with column pruning and filters, then run once and tune from the Spark UI. In my project our job clusters were r6g memory-optimised with autoscaling from 2 to 10 workers, which fit our joins."

---

## Q19. How do you make a pipeline idempotent and safe to re-run?

**Simple idea**
**Idempotent** = running the same job **once or five times gives the same result**. No duplicates, no missing data.

**Analogy**
A lift button: pressing it 5 times still calls the lift once.

**How**
1. **MERGE instead of blind append**, using a business key.
2. **Overwrite only the slice you process**:
   ```python
   (df.write.mode("overwrite")
      .option("replaceWhere", "run_date = '2026-11-01'")
      .saveAsTable("silver.sales"))
   ```
   Rerun for that date replaces the same date, nothing else.
3. **Parameterise the run date** (`run_date` parameter) instead of using `current_date()` inside code, so a rerun for an old day is possible (backfill).
4. **Checkpoints** for streaming / Auto Loader: already-processed files are not read again.
5. **De-duplicate** in silver with `ROW_NUMBER()` on the key.
6. **Delta ACID**: a failed write does not leave half data, so you just rerun.
7. **Audit / control table**: record the run, status, row counts, and last version processed.

**🎤 Say this in the interview**
"I design every task so a rerun gives the same result. I use MERGE on business keys or `replaceWhere` overwrites for just the processed date, I pass the run date as a parameter instead of using the current date in code, and I de-duplicate in silver. Delta's ACID writes mean a failed task leaves no partial data, so the fix is simply to rerun. For incremental sources I rely on checkpoints or stored versions. In my project this mattered because the Workflow had 14 dependent tasks and we needed to rerun from a failed step safely."

---

## Q20. How do you handle a failed task in a 14-task Workflow? (repair run, retries, alerts)

**Simple idea**
- **Repair run**: in Databricks Workflows, you can rerun **only the failed task and the tasks after it**. The successful tasks are **not** rerun. Saves time and money.
- **Retries**: set per task, e.g. 2 retries with a wait. Good for **temporary** errors (network, spot instance lost).
- **Timeouts**: stop a task that runs too long.
- **Alerts / notifications**: email, Slack, Teams or a webhook on failure, or when a run is late (duration threshold).
- **Run-if conditions**: e.g. a cleanup or alert task with `run_if = AT_LEAST_ONE_FAILED`.

**Analogy**
A 14-step assembly line. If step 9 breaks, you fix step 9 and continue from step 9, you do not rebuild the car from step 1.

**Steps when it fails**
1. Get the alert, open the run, read the **error and the task logs**.
2. Decide: **temporary** (rerun) or **real bug / bad data** (fix first).
3. Fix code (via PR + deploy) or data.
4. Use **Repair run** to rerun the failed task and downstream tasks.
5. Validate outputs (counts / recon), then tell stakeholders.
6. Prevent next time: add a test, a DQ check, or a retry.

**Tiny example (job YAML in Asset Bundle)**
```yaml
tasks:
  - task_key: silver_sales
    depends_on: [{task_key: bronze_sales}]
    max_retries: 2
    min_retry_interval_millis: 60000
    timeout_seconds: 3600
email_notifications:
  on_failure: [data-team@company.com]
```

**🎤 Say this in the interview** (honest version)
"Our Workflow had about 14 dependent tasks. When a task fails, I first read the error to see if it is transient or a real issue. After fixing it, I use Repair run, which reruns only the failed task and everything downstream, not the tasks that already succeeded. That works because each task is idempotent. For production I set failure notifications, timeouts, and retries on tasks that can hit temporary errors, and after the repair I validate the outputs with our reconciliation checks."

---

# 🔴 Round 3: Senior level (platform + leadership)

## Q21. CI/CD on Databricks: Asset Bundles, GitHub Actions, dev → prod flow

**Simple idea**
- **CI** (Continuous Integration) = every code change is **checked automatically** (lint, unit tests, bundle validate).
- **CD** (Continuous Deployment) = after approval, code is **deployed automatically** to the next environment.
- **Databricks Asset Bundles (DAB)** = a `databricks.yml` file that describes your jobs, clusters, notebooks and pipelines **as code**, with **targets** like `dev` and `prod`.
- **GitHub Actions** = runs the CI/CD steps on GitHub when you open a PR or merge.

**Analogy**
DAB = the recipe card for the whole kitchen setup. GitHub Actions = a robot chef that tests the recipe on every change, and cooks it in the real restaurant only after the manager approves.

**Flow**
1. Create a **feature branch**, write code.
2. Open a **Pull Request** to `main`. GitHub Actions runs: `flake8/ruff` lint, `pytest`, `databricks bundle validate`, deploy to **dev** target.
3. Teammate **reviews and approves**; merge to main.
4. On merge (or a release tag), Actions runs `databricks bundle deploy -t prod` using a **service principal** (Q22).
5. Prod jobs are updated; nobody edits prod by hand.

**Tiny example**
```yaml
# databricks.yml
bundle: {name: sales_pipeline}
targets:
  dev:  {mode: development, workspace: {host: https://dev.cloud.databricks.com}}
  prod: {mode: production,  workspace: {host: https://prod.cloud.databricks.com}}
```
```yaml
# .github/workflows/deploy.yml (part)
- run: pip install -r requirements.txt && pytest tests/
- uses: databricks/setup-cli@main
- run: databricks bundle deploy -t prod
  env:
    DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
    DATABRICKS_CLIENT_ID: ${{ secrets.SP_CLIENT_ID }}
    DATABRICKS_CLIENT_SECRET: ${{ secrets.SP_CLIENT_SECRET }}
```

**🎤 Say this in the interview**
"We used Databricks Asset Bundles to define jobs and code as YAML, with dev and prod targets, and GitHub Actions for CI/CD. On a pull request, the pipeline runs lint, pytest unit tests and `bundle validate`, and deploys to dev. After review and merge, it deploys to prod with `bundle deploy -t prod` using a service principal, so nobody changes prod manually and every change is reviewed and traceable in Git. I have also built this end to end in my own repo with a failing test blocking a PR."

---

## Q22. Service principals: why not use personal tokens in prod?

**Simple idea**
A **service principal (SP)** = a **robot account** for apps and jobs, not a human.

**Problems with personal tokens (PAT) in prod**
- If the person **leaves the company**, their account is disabled and **prod jobs break**.
- The job has **all the person's permissions** (too much access).
- Audit logs show the person, not "the pipeline".
- Tokens are often long-lived and get copied around.

**With a service principal**
- Jobs **run as** the SP (`run_as` in bundle).
- Give it **only the grants it needs** (least privilege).
- Use **OAuth machine-to-machine** (client id + secret, short-lived tokens), stored in a **secret store** (GitHub secrets, Databricks secret scope, AWS Secrets Manager).
- Rotate secrets regularly.

**Analogy**
Personal token = giving your house key to the cleaning company. Service principal = a separate key that only opens the kitchen, and you can cancel it anytime.

**🎤 Say this in the interview**
"In production, jobs and CI/CD should run as a service principal, not a person. If a personal token is used and that person leaves or changes role, pipelines break, and the job gets more permissions than it needs. A service principal gets least-privilege grants in Unity Catalog, authenticates with OAuth M2M with short-lived tokens, and its secrets live in a secret store. Audit logs then clearly show the pipeline identity. In my practice repo I used tokens to learn, and I know the right production pattern is a service principal."

---

## Q23. DLT (Lakeflow Declarative Pipelines) vs plain notebooks + Workflows

**Simple idea**
- **Notebooks + Workflows**: you write **how** to do everything: read, transform, write, MERGE, checkpoints, retries, order of tasks. Full control.
- **DLT / Lakeflow Declarative Pipelines**: you write **what** each table should be (a query). Databricks figures out the order, runs it, handles checkpoints, retries, and **data quality expectations**, and shows a lineage graph.

**Tiny example (DLT in Python)**
```python
import dlt

@dlt.table
def bronze_sales():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format","parquet").load("s3://.../sales")

@dlt.table
@dlt.expect_or_drop("valid_id", "order_id IS NOT NULL")
def silver_sales():
    return dlt.read_stream("bronze_sales").dropDuplicates(["order_id"])
```

**Analogy**
Notebooks = driving a manual car: you control every gear. DLT = an automatic car: you say the destination, it handles the gears.

| | Notebooks + Workflows | DLT / Lakeflow |
|---|---|---|
| Style | Imperative (how) | Declarative (what) |
| Order of steps | You set `depends_on` | Auto from table references |
| Data quality | Write your own checks | Built-in expectations |
| SCD / CDC | Write MERGE | `APPLY CHANGES` / auto CDC |
| Control & custom logic | Full | Less |
| Good for | Complex custom logic, migrations, non-table tasks | Standard ingest → clean → aggregate, streaming |

**🎤 Say this in the interview**
"DLT, now called Lakeflow Declarative Pipelines, lets me declare tables as queries, and Databricks handles dependencies, incremental processing, retries and expectations. Notebooks with Workflows give full control and suit custom logic. In my project we used notebooks and Workflows because we were translating SAS logic step by step into PySpark and needed tight control over each step. For a new standard ingestion pipeline, especially streaming or CDC, I would consider DLT because of built-in quality checks and APPLY CHANGES."

---

## Q24. Data quality: expectations, recon checks, what you do with bad rows

**Simple idea**: check data **at every layer**, and **never silently drop** bad rows.

**Types of checks**
- **Schema**: right columns and types.
- **Nulls** in key columns.
- **Duplicates** on the primary key.
- **Ranges / rules**: amount ≥ 0, dates not in the future, valid codes.
- **Counts**: source rows = bronze rows; row counts not suddenly 50% lower.
- **Reconciliation**: compare totals and rows between source and target (or old SAS vs new PySpark).
- **Freshness**: data arrived on time.

**What to do with bad rows**
1. **Quarantine**: write them to a `_rejected` / quarantine table with a `reject_reason` column.
2. Good rows continue.
3. If bad rows are above a threshold (e.g. > 1%), **fail the job** and alert.
4. Report, fix at source, then reprocess.

**Tools**
- DLT expectations: `@dlt.expect` (warn), `expect_or_drop` (drop), `expect_or_fail` (stop).
- Delta **CHECK constraints**: `ALTER TABLE t ADD CONSTRAINT pos CHECK (amount >= 0)`.
- Custom PySpark checks, dbt tests, Great Expectations, Lakehouse Monitoring.

**Tiny example**
```python
from pyspark.sql import functions as F
checked = df.withColumn("reject_reason",
    F.when(F.col("order_id").isNull(), "null_id")
     .when(F.col("amount") < 0, "negative_amount"))
checked.filter("reject_reason IS NOT NULL").write.mode("append").saveAsTable("silver.sales_rejected")
good = checked.filter("reject_reason IS NULL").drop("reject_reason")
```

**Analogy**
Quality check at a factory: bad items go into a separate "rejected" box with a label saying why. If too many are bad, the line is stopped.

**🎤 Say this in the interview**
"I check schema, nulls on keys, duplicates, business rules and row counts at each layer. Bad rows are not silently dropped: they go to a quarantine table with a reject reason, and if the reject rate crosses a threshold, the job fails and alerts. In my project, data quality was central: we checked nulls, duplicates and counts, and our team's Python recon utility compared two DataFrames on the primary key and produced an HTML report showing missing rows and column-level mismatches. In DLT I would use expectations for the same thing."

---

## Q25. Streaming: checkpoints, exactly-once, trigger types, watermarking

**Simple idea**
- **Checkpoint**: a folder where the stream **saves its progress** (which data it already processed, and state). If it crashes, it restarts from there. **Each stream needs its own checkpoint**. Never delete it by mistake.
- **Exactly-once**: every record affects the output **once**, even after failures. Spark gets this by: replayable source (Kafka offsets / files) + checkpoint + **idempotent sink** (Delta is transactional). With `foreachBatch` + MERGE, also make the merge idempotent.
- **Trigger types**:
  - default / `processingTime="1 minute"`: micro-batch every interval (near real-time).
  - `availableNow=True`: process everything new, then **stop** (good for scheduled jobs, cheaper).
  - `continuous`: very low latency, experimental, limited operations.
  - **Real-time mode** (newer Databricks feature) for millisecond latency.
- **Watermark**: tells Spark **how long to wait for late data**. For example `withWatermark("event_time", "10 minutes")`: data more than 10 min late is dropped, and old state is cleaned. Without it, state for aggregations grows forever.

**Analogy**
- Checkpoint = a bookmark in your book.
- Watermark = "the exam hall door closes 10 minutes after start; late students are not allowed".

**Tiny example**
```python
events = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", "...").option("subscribe", "bets")
          .load())

agg = (parsed.withWatermark("event_time", "10 minutes")
       .groupBy(F.window("event_time", "5 minutes"), "market_id").count())

(agg.writeStream.outputMode("append")
    .option("checkpointLocation", "/Volumes/main/chk/bets_agg")
    .trigger(processingTime="1 minute")
    .toTable("gold.bets_per_5min"))
```

**🎤 Say this in the interview**
"In Structured Streaming, the checkpoint stores offsets and state so the stream can restart exactly where it stopped. Exactly-once comes from a replayable source like Kafka, the checkpoint, and an idempotent transactional sink like Delta. For triggers, I use processingTime micro-batches for near real-time and availableNow for scheduled incremental runs that stop when done. For event-time aggregations I add a watermark so late data is handled within a limit and state does not grow forever. My client project was batch, and I have practised streaming hands-on; I know a sportsbook platform like yours relies on Kafka streaming, so this is an area I am actively building."

(Use the last sentence only in the Flutter interview.)

---

## Q26. Cost control: cluster policies, auto-terminate, spot instances, job vs all-purpose

**Simple idea**: pay only for what you need.

**Main levers**
1. **Job clusters for production**, not all-purpose (lower DBU rate, shut down after the job).
2. **Auto-terminate** on all-purpose clusters (e.g. 30 minutes idle).
3. **Autoscaling** with a sensible min and max (like our 2 to 10 workers).
4. **Spot instances** for workers (much cheaper on AWS), with **on-demand driver** and "fall back to on-demand", so losing a spot node does not kill the job.
5. **Cluster policies**: admins set rules: max workers, allowed instance types, auto-terminate required, tags required. Stops people creating huge clusters.
6. **Tags** (team, project, env) + **system tables** (`system.billing.usage`) to see who spends what. **Budgets / alerts**.
7. **Right instance type**: Graviton (r6g/m6g) is cheaper per performance on AWS.
8. **Serverless** for short / spiky jobs and SQL, so no idle clusters.
9. **Code efficiency**: OPTIMIZE, clustering, filter early, avoid Python UDFs, Photon where it saves time.

**Tiny example (policy, part)**
```json
{
  "autotermination_minutes": {"type": "range", "maxValue": 60},
  "autoscale.max_workers":   {"type": "range", "maxValue": 10},
  "custom_tags.team":        {"type": "fixed", "value": "data-eng"}
}
```

**Analogy**
Like controlling electricity bills at home: switch off lights when you leave (auto-terminate), use LED bulbs (Graviton/Photon), and have a family rule (policy) that nobody runs AC at 16°C all day.

**🎤 Say this in the interview**
"Production runs on job clusters with autoscaling; in my project that was r6g Graviton job clusters from 2 to 10 workers. All-purpose clusters are only for development with auto-terminate. On top of that I would use spot workers with an on-demand driver and fallback, cluster policies to cap size and enforce tags, and system billing tables with budgets to track spend per team. Often the biggest saving is in the code itself: reading less data with clustering and filters, and avoiding UDFs."

---

## Q27. Security: PII masking, row filters, column masks in Unity Catalog

**Simple idea**
- **PII** = personal data (name, email, phone, card number). Not everyone should see it.
- **Column mask**: a SQL function attached to a column. It decides **what value a user sees**: real value for allowed groups, masked value (`****`) for others.
- **Row filter**: a SQL function attached to a table. It decides **which rows a user can see** (e.g. only their region).
- Both are applied **automatically on every query**. No need for separate views.
- Plus: **grants** (least privilege), **tags** on PII columns, attribute-based access (ABAC) policies, audit logs.

**Tiny example**
```sql
-- column mask
CREATE FUNCTION gov.mask_email(email STRING)
RETURN CASE WHEN is_account_group_member('pii_readers') THEN email
            ELSE concat('***@', split(email, '@')[1]) END;

ALTER TABLE silver.customers ALTER COLUMN email SET MASK gov.mask_email;

-- row filter
CREATE FUNCTION gov.region_filter(region STRING)
RETURN is_account_group_member('admins') OR region = 'APAC';

ALTER TABLE silver.customers SET ROW FILTER gov.region_filter ON (region);
```

**Analogy**
Column mask = a bank statement showing only the last 4 digits of the card. Row filter = a branch manager can see only their branch's customers.

**🎤 Say this in the interview**
"In Unity Catalog I protect PII with column masks, which are SQL functions that return the real value only for authorised groups and a masked value for everyone else, and row filters, which limit rows by user or group, for example by region. They apply on every query, so we do not need many secure views. I combine that with least-privilege grants, tagging of PII columns, and audit logs. I have implemented column masking hands-on in practice."

---

## Q28. Lakehouse Federation: when do you query a source directly vs ingest it?

**Simple idea**
**Lakehouse Federation** lets you **query an outside database** (Oracle, PostgreSQL, MySQL, SQL Server, Snowflake, Redshift...) **from Databricks without copying the data**. You create a **connection** and a **foreign catalog** in Unity Catalog, then use normal SQL.

```sql
CREATE CONNECTION oracle_conn TYPE oracle
OPTIONS (host '...', port '1521', user secret('scope','user'), password secret('scope','pwd'));

CREATE FOREIGN CATALOG oracle_cat USING CONNECTION oracle_conn OPTIONS (service_name 'ORCL');

SELECT * FROM oracle_cat.sales_schema.customers WHERE country = 'IN';
```
Filters are **pushed down** to the source where possible.

**Analogy**
Federation = calling a friend to ask a question (fast, no copy). Ingestion = photocopying their notebook so you can read it anytime without disturbing them.

**Query directly (federation) when**
- Small / medium reference data, occasional queries, ad-hoc exploration, PoCs.
- You need the **latest** value, and volumes are low.
- You want governance (UC grants, lineage) without building a pipeline yet.

**Ingest into Delta when**
- Large data or **heavy, repeated** queries and joins.
- You must **not load the source system** (production OLTP).
- You need **history**, time travel, or fast performance.
- Network latency/cost is high.

Often both: federate first, then ingest (copy into bronze) the tables that are heavily used.

**🎤 Say this in the interview**
"Lakehouse Federation lets Databricks query external databases through a UC connection and foreign catalog, with pushdown and Unity Catalog governance, without copying data. In my project we accessed Oracle through federation over AWS PrivateLink, so traffic stayed private. I use federation for smaller reference tables and ad-hoc access, and I ingest into Delta when data is large, queried often, or when we must not put load on the source system or need history."

---

## Q29. Design question: build a pipeline for 500 GB a day of event data, end to end

**How to answer a design question** (use this order every time)
1. **Ask questions** (requirements) 2. **Draw the flow** 3. **Explain each layer** 4. **Scale, cost, failures, quality, security** 5. **Trade-offs**.

**1. Questions to ask first**
- Batch or real-time? What latency is needed (seconds, minutes, daily)?
- Source: Kafka, files in S3, APIs? Format: JSON, Avro?
- Who uses it: dashboards, ML, other teams? Retention? PII?

**2. Flow**
```
Apps/Events → Kafka (or Kinesis / files in S3)
   → Bronze: Auto Loader or Structured Streaming → Delta (raw, append-only)
   → Silver: parse JSON, dedupe (event_id + watermark), clean, join dims, quarantine bad rows
   → Gold: aggregates (per hour / per user / per market), liquid clustered
   → Consumers: Databricks SQL warehouse dashboards, ML features, Delta Sharing
Orchestration: Workflows or Lakeflow pipeline, deployed with Asset Bundles + GitHub Actions
Governance: Unity Catalog (grants, masks, lineage), service principal
```

**3. Key numbers**
- 500 GB/day ≈ **21 GB/hour ≈ 6 MB/sec**. Very manageable for Spark.
- Partitions: 500 GB / 128 MB ≈ 4,000 per day if processed in one batch; or small micro-batches if streaming.

**4. Design choices**
- **Ingest**: Kafka → Structured Streaming with checkpoints for real-time; or Auto Loader with `availableNow` every 15 min/hour if near-real-time is enough (cheaper).
- **Storage**: Delta, **liquid clustering** on `event_date, event_type` or user id; auto compaction to avoid small files.
- **Dedup**: `dropDuplicates(["event_id"])` with a **watermark** for late events.
- **Schema change**: Auto Loader `addNewColumns` / `_rescued_data` in bronze.
- **Quality**: expectations; quarantine table; alert on volume drop.
- **Reliability**: idempotent writes, checkpoints, retries, repair runs, alerts.
- **Cost**: job compute or serverless, autoscaling, spot workers, Photon for gold aggregates, VACUUM + retention rules.
- **Security**: PII columns masked, least privilege, service principals.
- **Monitoring**: stream metrics (input rate vs processing rate), job duration, freshness SLAs, Lakehouse Monitoring.

**5. Trade-offs to say**
- Streaming = lower latency but always-on cost. Scheduled `availableNow` = cheaper, minutes of delay.
- DLT = less code and built-in quality; notebooks = more control.

**🎤 Say this in the interview** (short version)
"First I'd confirm latency, source and consumers. Assuming events land in Kafka, I'd stream them with Structured Streaming into a bronze Delta table, append-only with checkpoints. In silver I'd parse, de-duplicate on event id with a watermark, apply data-quality expectations and send bad rows to quarantine. Gold has hourly and daily aggregates with liquid clustering, served through a SQL warehouse. 500 GB a day is only about 6 MB a second, so a moderate autoscaling cluster is enough; if minutes of delay are fine I'd use availableNow triggers to save cost. Everything is governed in Unity Catalog, runs as a service principal, deployed with Asset Bundles and GitHub Actions, with alerts on failures, freshness and volume drops."

---

## Q30. You're migrating SAS to PySpark: how do you prove the outputs match?

**Simple idea**: run **both** (old SAS and new PySpark) on the **same input**, and **compare outputs automatically**, at several levels.

**Step by step**
1. **Same input**: freeze one input snapshot (same date, same files) for SAS and PySpark.
2. **Understand the SAS logic first**: DATA steps, PROC SQL, `RETAIN`, `BY` group processing (`FIRST.`/`LAST.`), formats, missing values. Write the mapping (SAS step → PySpark function).
3. **Level 1: row counts** for each output table.
4. **Level 2: schema**: column names, types, decimal precision.
5. **Level 3: aggregates**: SUM, MIN, MAX, COUNT DISTINCT per key column and per group.
6. **Level 4: row-by-row on primary key**:
   - rows only in SAS, rows only in PySpark (anti-joins),
   - for matching keys, **column-level differences**.
7. **Handle known differences**: SAS missing (`.`) vs Spark `NULL`; sorting differences; rounding (use a tolerance like 0.01); date formats; trailing spaces; case.
8. **Report and sign-off**: share a report with match % and explain every mismatch; business signs off; then run in **parallel** for a few cycles before switching off SAS.

**Tiny example (core compare idea)**
```python
pk = ["customer_id"]
only_sas  = sas_df.join(spark_df, pk, "left_anti")
only_pys  = spark_df.join(sas_df, pk, "left_anti")
both = sas_df.alias("a").join(spark_df.alias("b"), pk)
diff_amount = both.filter(F.abs(F.col("a.amount") - F.col("b.amount")) > 0.01)
print(only_sas.count(), only_pys.count(), diff_amount.count())
```

**Analogy**
Like checking two students' answer sheets for the same exam: first count the answers, then check totals, then compare question by question.

**🎤 Say this in the interview**
"We ran SAS and PySpark on the same input snapshot and compared outputs at several levels: row counts, schema, aggregates per key, and then row by row on the primary key. Our team had a Python reconciliation utility: you give it two DataFrames and the primary key, and it produces an HTML report showing rows missing on either side and column-level mismatches. We handled known differences like SAS missing values versus nulls and rounding with a tolerance. Every mismatch was explained or fixed before business sign-off, and only then did we switch the job to production."

---

# ✅ Quick revision: one line per question

| # | One-line answer |
|---|---|
| 1 | Lakehouse = cheap lake storage + warehouse features (ACID, governance) via Delta + UC |
| 2 | Delta = Parquet + `_delta_log` JSON commits + checkpoints → ACID, time travel |
| 3 | All-purpose = dev, job cluster = prod, serverless = fast start, no tuning |
| 4 | Bronze raw, silver clean/joined, gold business tables |
| 5 | `MERGE INTO ... WHEN MATCHED UPDATE / NOT MATCHED INSERT`, dedupe source first |
| 6 | `VERSION AS OF`, `TIMESTAMP AS OF`, `RESTORE TABLE`, limited by VACUUM |
| 7 | OPTIMIZE = compact, Z-ORDER = co-locate for skipping, VACUUM = delete old files |
| 8 | Auto Loader = incremental with checkpoint + schema evolution; `spark.read` = full read |
| 9 | DROP managed = data gone; DROP external = only metadata gone |
| 10 | Widgets/job parameters in, `taskValues.set/get` between tasks |
| 11 | Metastore → catalog → schema → table; credential = IAM role, location = S3 path |
| 12 | Enforcement default; `mergeSchema`, `addNewColumns`, `_rescued_data` |
| 13 | Compare runs → Spark UI slow stage → skew/spill/shuffle/small files → fix one at a time |
| 14 | Liquid clustering default; partition only huge low-cardinality; Z-ORDER legacy |
| 15 | Close old row + insert new; MERGE with merge-key trick or APPLY CHANGES |
| 16 | CDF = row-level changes per version; use when small % changes in big table |
| 17 | Photon = C++ vectorised engine; AQE = runtime re-plan (coalesce, broadcast, skew) |
| 18 | 1 TB / 128 MB ≈ 8,000 tasks → cores by waves → memory-optimised + autoscale |
| 19 | MERGE / replaceWhere, run_date param, dedupe, checkpoints → rerun-safe |
| 20 | Read error → fix → Repair run (failed + downstream) → validate; retries + alerts |
| 21 | DAB YAML + GitHub Actions: PR → tests → dev; merge → prod via SP |
| 22 | SP = robot account, least privilege, OAuth, does not break when people leave |
| 23 | DLT = declarative + expectations; notebooks = full control |
| 24 | Check nulls/dupes/rules/counts, quarantine bad rows, fail above threshold, recon |
| 25 | Checkpoint = progress, exactly-once = replay + checkpoint + idempotent sink, watermark = late data limit |
| 26 | Job clusters, auto-terminate, autoscale, spot, policies, tags + billing tables |
| 27 | Column masks + row filters (SQL functions), grants, tags, audit |
| 28 | Federation for small/ad-hoc/latest; ingest for big/frequent/history |
| 29 | Kafka → bronze stream → silver dedupe+DQ → gold clustered → SQL; 500 GB/day ≈ 6 MB/s |
| 30 | Same input → counts → schema → aggregates → PK row compare (recon HTML report) → sign-off |
