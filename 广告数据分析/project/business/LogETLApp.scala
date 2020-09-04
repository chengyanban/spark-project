package com.chengyanban.project.business

import com.chengyanban.project.utils.{IPUtils, KuduUtils, SQLUtils, SchemaUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}


object LogETLApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("LogETLApp").getOrCreate()

    var jsonDF = spark.read.json("data/data-test.json")
    //jsonDF.printSchema()
    //jsonDF.show(false)

    import spark.implicits._
    val ipRDD = spark.sparkContext.textFile("data/ip.txt")
    val ipRuleDF = ipRDD.map(x => {
      val splits = x.split("[|]")
      val ip_start = splits(2).toLong
      val ip_end = splits(3).toLong
      val province = splits(6)
      val city = splits(7)
      val isp = splits(9)
      (ip_start, ip_end, province, city, isp)
    }).toDF("start_ip", "end_ip", "province", "city", "isp")
    //ipRuleDF.show()

    import org.apache.spark.sql.functions._

    def getLongIp = udf((ip:String) => {
      IPUtils.ip2Long(ip)
    })

    jsonDF = jsonDF.withColumn("ip_long", getLongIp($"ip"))
    //jsonDF.show(false)

    //jsonDF.join(ipRuleDF, jsonDF("ip_long").between(ipRuleDF("start_ip"),ipRuleDF("end_ip"))).show(false)

    jsonDF.createOrReplaceTempView("logs")
    ipRuleDF.createOrReplaceTempView("ips")
    val sql = SQLUtils.SQL

    val result = spark.sql(sql)

    val KUDU_MASTERS = "hadoop000"
    val tableName = "ods"
    val partitionID = "ip"
    val schema = SchemaUtils.ODSSchema

    KuduUtils.sink(result,tableName, KUDU_MASTERS, schema,partitionID)

    spark.stop()

  }
}
