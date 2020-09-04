package com.chengyanban.project.utils

import java.util

import org.apache.kudu.Schema
import org.apache.kudu.client.{CreateTableOptions, KuduClient}
import org.apache.spark.sql.{DataFrame, SaveMode}

object KuduUtils {
  def sink(data:DataFrame,
           tableName:String,
           master:String,
           schema:Schema,
           partitionID:String
          ): Unit ={
    val client: KuduClient = new KuduClient.KuduClientBuilder(master).build()

    if (client.tableExists(tableName)){
      client.deleteTable(tableName)
    }

    val options: CreateTableOptions = new CreateTableOptions()
    options.setNumReplicas(1)

    val parcols: util.LinkedList[String] = new util.LinkedList[String]()
    parcols.add(partitionID)
    options.addHashPartitions(parcols,3)

    client.createTable(tableName, schema, options)

    data.write.mode(SaveMode.Append).format("org.apache.kudu.spark.kudu")
      .option("kudu.master", master)
      .option("kudu.table", tableName)
      .save()

  }

}
