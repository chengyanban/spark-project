package com.chengyanban.project.business

import com.chengyanban.project.`trait`.DataProcess
import com.chengyanban.project.utils.{DateUtils, KuduUtils, SQLUtils, SchemaUtils}
import org.apache.spark.sql.SparkSession

object ProvinceCityStatProcess extends DataProcess{
  override def process(spark:SparkSession): Unit = {

    val sourceTableName = DateUtils.getTableName("ods", spark)
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
    val tableName = DateUtils.getTableName("province_city_stat", spark)
    val partitionID = "provincename"
    val schema = SchemaUtils.ProvinceCitySchema

    KuduUtils.sink(result,tableName, KUDU_MASTERS, schema,partitionID)
  }
}
