package com.chengyanban.project.business

import com.chengyanban.project.utils.{KuduUtils, SQLUtils, SchemaUtils}
import org.apache.spark.sql.SparkSession

object ProvinceCityStatApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("ProvinceCityStatApp").getOrCreate()

    val sourceTableName = "ods"
    val masterAdress = "hadoop000"

    val odsDF = spark.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.master", masterAdress)
      .option("kudu.table", sourceTableName)
      .load()
    //odsDF.show()

    odsDF.createOrReplaceTempView("ods")
    val result = spark.sql(SQLUtils.PROVINCE_CITY_SQL)
    //result.show()

    val KUDU_MASTERS = "hadoop000"
    val tableName = "Province_City_Stat"
    val partitionID = "provincename"
    val schema = SchemaUtils.ProvinceCitySchema

    KuduUtils.sink(result,tableName, KUDU_MASTERS, schema,partitionID)
    spark.stop()
  }
}
