# Databricks notebook source
# MAGIC %md
# MAGIC ## Lakeflow Job Orchestration
# MAGIC
# MAGIC This orchestration workflow coordinates the execution of the Formula 1 pipeline across Bronze, Silver, and Gold layers.
# MAGIC
# MAGIC The process manages notebook dependencies and execution order using Databricks Lakeflow Jobs.
# MAGIC
# MAGIC ![Lakeflow Workflow](../07-images/formula1-lakeflow-pipeline.png)

# COMMAND ----------

