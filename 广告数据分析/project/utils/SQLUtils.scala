package com.chengyanban.project.utils

object SQLUtils {
  lazy val SQL = "select " +
    "logs.ip ," +
    "logs.sessionid," +
    "logs.advertisersid," +
    "logs.adorderid," +
    "logs.adcreativeid," +
    "logs.adplatformproviderid" +
    ",logs.sdkversion" +
    ",logs.adplatformkey" +
    ",logs.putinmodeltype" +
    ",logs.requestmode" +
    ",logs.adprice" +
    ",logs.adppprice" +
    ",logs.requestdate" +
    ",logs.appid" +
    ",logs.appname" +
    ",logs.uuid, logs.device, logs.client, logs.osversion, logs.density, logs.pw, logs.ph" +
    ",ips.province as provincename" +
    ",ips.city as cityname" +
    ",ips.isp as isp" +
    ",logs.ispid, logs.ispname" +
    ",logs.networkmannerid, logs.networkmannername, logs.iseffective, logs.isbilling" +
    ",logs.adspacetype, logs.adspacetypename, logs.devicetype, logs.processnode, logs.apptype" +
    ",logs.district, logs.paymode, logs.isbid, logs.bidprice, logs.winprice, logs.iswin, logs.cur" +
    ",logs.rate, logs.cnywinprice, logs.imei, logs.mac, logs.idfa, logs.openudid,logs.androidid" +
    ",logs.rtbprovince,logs.rtbcity,logs.rtbdistrict,logs.rtbstreet,logs.storeurl,logs.realip" +
    ",logs.isqualityapp,logs.bidfloor,logs.aw,logs.ah,logs.imeimd5,logs.macmd5,logs.idfamd5" +
    ",logs.openudidmd5,logs.androididmd5,logs.imeisha1,logs.macsha1,logs.idfasha1,logs.openudidsha1" +
    ",logs.androididsha1,logs.uuidunknow,logs.userid,logs.iptype,logs.initbidprice,logs.adpayment" +
    ",logs.agentrate,logs.lomarkrate,logs.adxrate,logs.title,logs.keywords,logs.tagid,logs.callbackdate" +
    ",logs.channelid,logs.mediatype,logs.email,logs.tel,logs.sex,logs.age " +
    "from logs left join " +
    "ips on logs.ip_long between ips.start_ip and ips.end_ip "

  lazy val PROVINCE_CITY_SQL = "select provincename, cityname, count(1) as cnt from ods group by provincename,cityname"

  lazy val AREA_SQL_STEP1 = "select provincename,cityname, " +
    "sum(case when requestmode=1 and processnode >=1 then 1 else 0 end) origin_request," +
    "sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) valid_request," +
    "sum(case when requestmode=1 and processnode =3 then 1 else 0 end) ad_request," +
    "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and isbid=1 and adorderid!=0 then 1 else 0 end) bid_cnt," +
    "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 then 1 else 0 end) bid_success_cnt," +
    "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) ad_display_cnt," +
    "sum(case when requestmode=3 and processnode=1 then 1 else 0 end) ad_click_cnt," +
    "sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) medium_display_cnt," +
    "sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) medium_click_cnt," +
    "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>20000  then 1*winprice/1000 else 0 end) ad_consumption," +
    "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>20000  then 1*adpayment/1000 else 0 end) ad_cost " +
    "from ods group by provincename,cityname"

  lazy val AREA_SQL_STEP2 = "select provincename,cityname, " +
    "origin_request," +
    "valid_request," +
    "ad_request," +
    "bid_cnt," +
    "bid_success_cnt," +
    "bid_success_cnt/bid_cnt bid_success_rate," +
    "ad_display_cnt," +
    "ad_click_cnt," +
    "ad_click_cnt/ad_display_cnt ad_click_rate," +
    "ad_consumption," +
    "ad_cost from area_tmp " +
    "where bid_cnt!=0 and ad_display_cnt!=0"

  lazy val APP_SQL_STEP1 = "select appid,appname, " +
    "sum(case when requestmode=1 and processnode >=1 then 1 else 0 end) origin_request," +
    "sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) valid_request," +
    "sum(case when requestmode=1 and processnode =3 then 1 else 0 end) ad_request," +
    "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and isbid=1 and adorderid!=0 then 1 else 0 end) bid_cnt," +
    "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 then 1 else 0 end) bid_success_cnt," +
    "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) ad_display_cnt," +
    "sum(case when requestmode=3 and processnode=1 then 1 else 0 end) ad_click_cnt," +
    "sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) medium_display_cnt," +
    "sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) medium_click_cnt," +
    "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>20000  then 1*winprice/1000 else 0 end) ad_consumption," +
    "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>20000  then 1*adpayment/1000 else 0 end) ad_cost " +
    "from ods group by appid,appname"


  lazy val APP_SQL_STEP2 = "select appid,appname, " +
    "origin_request," +
    "valid_request," +
    "ad_request," +
    "bid_cnt," +
    "bid_success_cnt," +
    "bid_success_cnt/bid_cnt bid_success_rate," +
    "ad_display_cnt," +
    "ad_click_cnt," +
    "ad_click_cnt/ad_display_cnt ad_click_rate," +
    "ad_consumption," +
    "ad_cost from app_tmp " +
    "where bid_cnt!=0 and ad_display_cnt!=0"
}
