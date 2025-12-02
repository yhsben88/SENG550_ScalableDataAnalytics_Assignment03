# Assignment Rundown

1. From `assignment3/part0_environment` run: `docker-compose up -d`
   This will start Redis, Spark master, and Airflow (web UI at http://localhost:8081).
   Access airflow credentials by running `bash get_airflow_pass.sh`

2. Place the Kaggle dataset CSV into:
   `assignment3/part1_data_split/orders.csv.csv`

3. steps are followed according to assignment handout requirements.

# Part 2 Question

## Does Redis volatility hurt incremental processing?

Redis stores the _state_ (processed_max_dow). If Redis is lost:

- System impact: The incremental job will think nothing was processed (key missing) and will reprocess available folders from scratch.
- Consequences:
  - Duplicate processing: it will re-sum and overwrite the processed CSV (which is safe because final aggregation is idempotent if you overwrite).
  - If processed CSV persists, reprocessing is extra work but not inconsistent. However, if your merge logic incorrectly appended duplicates, duplicates might appear.
- Mitigation:
  - Keep a durable checkpoint (e.g., a small file in HDFS or S3) of processed_max_dow in addition to Redis.
  - Use Redis persistence (RDB/AOF) or run Redis with disk-backed persistence and replication.
  - Make aggregation idempotent: always overwrite the result for the same keys or deduplicate when merging.

Conclusion: Redis volatility can cause extra work but not necessarily data corruption if you implement idempotent aggregation and persist final processed CSVs.

# Part 4 Questions

## Redis caching / durability notes

**Q1: Advantages of caching predictions in Redis vs direct model inference**

- Advantages:
  - Ultra-low-latency reads (microseconds), able to serve thousands of requests/sec.
  - Offloads compute from the ML model serving endpoint; model can be retrained on schedule.
  - Simple key-value lookups scale horizontally (sharding).
- Disadvantages:
  - Staleness: cached predictions only update when pushed; if the model changes or new data arrives, cache must be refreshed.
  - Memory cost: storing predictions for many combinations (7 _ 24 _ #categories) may be large.
  - Cache misses: still need fallback to model inference.

**Q2: If Redis is cleared, does it significantly hurt system?**

- It depends on how critical latency/availability is:
  - If Redis is cleared, cached predictions are lost; the system must fallback to live model inference (higher latency) until cache is re-populated.
  - This causes a temporary performance hit rather than data inconsistency because predictions can be recomputed deterministically from the model and processed data.
- Mitigation:
  - Persist predictions to a durable store (e.g., S3, persistent DB) and repopulate Redis after restart.
  - Use Redis persistence options (RDB snapshots, AOF), and run Redis in a clustered/multi-AZ setup.
  - On startup, a background job repopulates Redis predictions from the saved model and processed dataset.

# Part 5 Question

## Comparative analysis: Apache Spark vs Spark on Databricks vs AWS Glue

**Apache Spark (open-source)**  
Spark is a flexible distributed data processing engine that you run on your own clusters or VMs. It provides direct control over configuration, versions, and integration. It's best when you need custom cluster tuning, close control of resources, or to avoid cloud vendor lock-in. The downside: you manage cluster provisioning, upgrades, and scaling.

**Spark on Databricks**  
Databricks offers a managed platform around Spark with a polished UI, optimizations (e.g., Photon, runtime tuned builds), integrated notebooks, job scheduling, and collaborative features. It reduces ops overhead and often yields better performance out-of-the-box. Tradeoffs: cost and vendor lock-in.

**AWS Glue**  
Glue is a serverless ETL platform that runs Spark under the hood (Glue uses a customized Spark runtime). It offers managed infrastructure, automatic scaling, built-in integrations with S3, Glue Catalog, and IAM. It’s convenient for AWS-centric pipelines but is less flexible than self-managed Spark or Databricks for advanced tuning. Glue is good for event-driven, serverless ETL at scale.
