package com.chengyanban.project.utils

import org.apache.spark.sql.SparkSession

object DateUtils {
  def getTableName(tableName:String, spark:SparkSession) = {
    val time = spark.sparkContext.getConf.get("spark.time")
    tableName + "_" + time
  }
}
