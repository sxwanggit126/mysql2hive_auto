package app

import meta.Meta
import org.apache.spark.sql.SparkSession
import util.ValueUtils
import app.MySQL2HiveMeta.msl_table_temp
object MysqltoHive {

  def main(args: Array[String]): Unit = {

    var msl_table = ""
    if (args != null && args.length > 0) {
      msl_table = args(0)
    } else {
      msl_table = "event"
    }

    val mySQL2HiveMeta: Meta = MySQL2HiveMeta.getMySQL2Hive(msl_table)
    val table = mySQL2HiveMeta.msl_table
    val dbase = mySQL2HiveMeta.msl_dbase
    msl_table_temp =msl_table
    println(table)
    val spark = SparkSession.builder()
      .config(ValueUtils.getStringValue("spark.default.partition"), "true")
      .config(ValueUtils.getStringValue("spark.default.mode"), "nonstrict")
      .enableHiveSupport()
      .appName(s"$dbase.$table")
//      .master(ValueUtils.getStringValue("spark.default.master"))
      .getOrCreate()

    //TODO...程序入口
        ProcessingAPP.mysql2hive(spark, dbase, table)
  }
}


