package com.dataframeoperations

import org.apache.spark.sql.SparkSession

case class Match(matchId: Int, player1: String, player2: String)

case class Player(name: String, birthYear: Int)

object ReadDataFramesOperations {

  val EMP_CSV_PATG = "D:/USA_Works/Hadoop_Spark_Training/SourceCode/emp.csv"
  val DEPT_CSV_PATG = "D:/USA_Works/Hadoop_Spark_Training/SourceCode/dept.csv"

  def readEmpFile(spark: SparkSession, debug: Int = 1): Unit = {

    val empDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(EMP_CSV_PATG);
    if (debug == 1) {
      empDf.printSchema();
      empDf.show(10, false)
    }
    empDf.createOrReplaceTempView("emp_tbl")

    var empId = "10"
    val df2 = spark.sql(""" SELECT * FROM emp_tbl WHERE empId= %s""".format(empId))
    df2.printSchema()
    df2.show(10)
  }

  def readDeptFile(spark: SparkSession, debug: Int = 1): Unit = {
    val deptDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(DEPT_CSV_PATG);

    if (debug == 1) {
      deptDf.printSchema();
      deptDf.show(10, false)
    }
    deptDf.createOrReplaceTempView("dept_tbl")

  }

  def jdbcCallEntityRead(spark: SparkSession, debug: Int = 1): Unit = {
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://xxxxxxxxx:3306")
      .option("dbtable", "xxxxxxxxxxxxxxx.user_tbl")
      .option("user", "root")
      .option("password", "devroot")
      .load()
    if (debug == 1) {

      jdbcDF.printSchema();
      jdbcDF.show(10, false)

    }
    jdbcDF.createOrReplaceTempView("user_tbl")

  }

  def callHiveTable(spark: SparkSession, debug: Int = 1): Unit = {


  }

  def matchDF(spark: SparkSession, debug: Int = 1): Unit = {
    val matches = Seq(
      Match(1, "John Wayne", "John Doe"),
      Match(2, "Ive Fish", "San Simon")
    )
    val matchesDf = spark.sqlContext.createDataFrame(matches)
    matchesDf.createOrReplaceTempView("matches")
  }

  def playerDF(spark: SparkSession, debug: Int = 1): Unit = {

    val players = Seq(
      Player("John Wayne", 1986),
      Player("Ive Fish", 1990),
      Player("San Simon", 1974),
      Player("John Doe", 1995)
    )

    val playersDf = spark.sqlContext.createDataFrame(players)
    playersDf.createOrReplaceTempView("players")
  }

  def joinAllDataFrame(spark: SparkSession, debug: Int = 1): Unit = {

    // Join emp_tbl & dept_tbl
    val joinwithempDeptDf = spark.sql(
      """  SELECT e.empId,e.empName,e.empAddress,
                  e.empEmail,e.empSal,d.deptId,
                  d.deptname FROM emp_tbl e INNER JOIN dept_tbl d
                  ON e.empId=d.empid """)
    if (debug == 1) {
      joinwithempDeptDf.printSchema()
      joinwithempDeptDf.show(10, false);
    }
    joinwithempDeptDf.createOrReplaceTempView("emp_dept_tbl")

    val joinWithJdbcUserTableDf = spark.sql(
      """ SELECT  emp_dept.empId,emp_dept.empName,emp_dept.empAddress,
                  emp_dept.empEmail,emp_dept.empSal,emp_dept.deptId,
                  emp_dept.deptname,user.gender,user.city
                  FROM emp_dept_tbl as emp_dept LEFT OUTER JOIN
                  user_tbl user ON emp_dept.empId=user.user_id
            """)
    if (debug == 1) {
      joinWithJdbcUserTableDf.printSchema()
      joinWithJdbcUserTableDf.show(10, false)
    }

    val matchPlayerDF = spark.sql(
      "select matchId, player1, player2, p1.birthYear, p2.birthYear, abs(p1.birthYear-p2.birthYear) " +
        "from matches m inner join  players p1 inner join players p2 " +
        "where m.player1 = p1.name and m.player2 = p2.name")
    if (debug == 1) {
      matchPlayerDF.printSchema()
      matchPlayerDF.show(10, false)
    }
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession();
    readEmpFile(spark, 0)
    readDeptFile(spark, 0);
    jdbcCallEntityRead(spark, 0)
    matchDF(spark, debug = 0)
    playerDF(spark, debug = 0)
    joinAllDataFrame(spark)
  }

}
