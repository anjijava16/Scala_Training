package com.dataframeoperations

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtils {

  def getSparkSession(): SparkSession = {

    lazy val sparkConf = new SparkConf()
      .setAppName("Learn Spark")
      .setMaster("local[*]")
      .set("spark.cores.max", "2")

    lazy val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    return spark;
  }
}
