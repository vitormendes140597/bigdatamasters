// Databricks notebook source
import org.apache.spark.sql.functions.{get_json_object,to_timestamp}

val inboundFile = "adl://adlsblobstudyeastus2dev1.azuredatalakestore.net/inbound/source-4-ds-train.json"
val bronzeDF = spark.read.text(inboundFile)
val bronzeDF2 = bronzeDF.withColumn("id",get_json_object($"value","$.id"))
                        .withColumn("createdAt", to_timestamp(get_json_object($"value","$.createdAt")))
                        .withColumn("updatedAt", to_timestamp(get_json_object($"value","$.updatedAt")))
// ENV VARIABLES
val dbName = "db_zap_project_bronze"
val tbName = "tb_zap_imoveis"
val tableId = s"$dbName.$tbName"

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS db_zap_project_bronze

// COMMAND ----------

if(!spark.catalog.tableExists(tableId)) {
  bronzeDF2.write
           .format("delta")
           .mode("append")
           .option("path","adl://adlsblobstudyeastus2dev1.azuredatalakestore.net/bronze")
           .saveAsTable(tableId)
} else {
 bronzeDF2.createOrReplaceTempView("vw_source")
 spark.sql(s"""
   MERGE INTO ${tableId} as target
   USING vw_source as source
   ON target.id = source.id
   WHEN MATCHED AND source.updatedAt > target.updatedAt THEN
    UPDATE SET *
   WHEN NOT MATCHED THEN
     INSERT *
 """)
}
