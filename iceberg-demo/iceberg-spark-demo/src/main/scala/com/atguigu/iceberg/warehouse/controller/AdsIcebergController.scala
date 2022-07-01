package com.atguigu.iceberg.warehouse.controller

import com.atguigu.iceberg.warehouse.service.AdsIcebergService
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AdsIcebergController {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.hadoop_prod.type", "hadoop")
      .set("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://mycluster/spark/warehouse")
      .set("spark.sql.catalog.catalog-name.type", "hadoop")
      .set("spark.sql.catalog.catalog-name.default-namespace", "default")
      .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .set("spark.sql.session.timeZone", "GMT+8")
      //      .setMaster("local[*]")
      .setAppName("ads_app")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    AdsIcebergService.queryDetails(sparkSession, "20190722")
  }
}