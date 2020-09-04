package com.chengyanban.project.`trait`

import org.apache.spark.sql.SparkSession

trait DataProcess {
  def process(spark:SparkSession)
}
