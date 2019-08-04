package com.dataframeoperations

import com.dataframeoperations.ReadDataFramesOperations.{EMP_CSV_PATG, readEmpFile}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object PassParameterToDataFrame {
  val EMP_CSV_PATG = "D:/USA_Works/Hadoop_Spark_Training/SourceCode/emp.csv"


  def readEmpFile(spark: SparkSession, debug: Int = 1): Unit = {

    val empDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(EMP_CSV_PATG);

    empDf.createOrReplaceTempView("emp_tbl")

    var empId = "10"
    val selectByEmpIdDf = spark.sql(""" SELECT * FROM emp_tbl WHERE empId= %s""".format(empId))
    selectByEmpIdDf.printSchema()
    selectByEmpIdDf.show(10)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession();
    readEmpFile(spark, 0)
  }
}
