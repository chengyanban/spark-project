package com.chengyanban.project.business

import com.chengyanban.project.`trait`.DataProcess
import com.chengyanban.project.utils.{DateUtils, KuduUtils, SQLUtils, SchemaUtils}
import org.apache.spark.sql.SparkSession

object AppStatProcess extends DataProcess{
  override def process(spark:SparkSession): Unit ={
    val sourceTableName = DateUtils.getTableName("ods", spark)
    val masterAdress = "hadoop000"

    val odsDF = spark.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.master", masterAdress)
      .option("kudu.table", sourceTableName)
      .load()
    //odsDF.show()
    odsDF.createOrReplaceTempView("ods")
    val resultTmp = spark.sql(SQLUtils.APP_SQL_STEP1 )
    //resultTmp.show()
    resultTmp.createOrReplaceTempView("app_tmp")
    val result = spark.sql(SQLUtils.APP_SQL_STEP2 )
    //result.show()

    val KUDU_MASTERS = "hadoop000"
    val tableName = DateUtils.getTableName("app_stat", spark)
    val partitionID = "appid"
    val schema = SchemaUtils.APPSchema

    KuduUtils.sink(result,tableName, KUDU_MASTERS, schema,partitionID)
  }

}
