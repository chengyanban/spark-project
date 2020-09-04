package com.chengyanban.project.utils

object IPUtils {

  def ip2Long(ip:String)={
    val splits = ip.split("[.]")
    var ipNum = 0L

    for (i <- 0.until(splits.length)){
      ipNum = splits(i).toLong | ipNum << 8L
    }

    ipNum
  }

  def main(args: Array[String]): Unit = {
    println(ip2Long("123.23.3.11"))
  }
}
