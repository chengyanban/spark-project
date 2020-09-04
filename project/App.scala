package com.chengyanban.project

import com.chengyanban.project.business.{AppStatProcess, AreaStatProcess, LogETLProcessor, ProvinceCityStatProcess}
import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.Logging
import org.apache.commons.lang3.StringUtils

object App extends Logging{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val time = spark.sparkContext.getConf.get("spark.time")  // spark框架只认以spark.开头的参数，否则系统不识别
    if(StringUtils.isBlank(time)) {  // 如果是空，后续的代码就不应该执行了
      logError("处理批次不能为空....")
      System.exit(0)
    }
    LogETLProcessor.process(spark)

    ProvinceCityStatProcess.process(spark)

    AreaStatProcess.process(spark)

    AppStatProcess.process(spark)

    spark.stop()
  }
}
