package app

import java.util.Date

import meta.Meta
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import scalikejdbc.DB
import util.ValueUtils
import scalikejdbc._
import scalikejdbc.config.DBs

import scala.collection.mutable.ArrayBuffer

object ProcessingAPP {

  val mySQL2HiveMeta: Meta = MySQL2HiveMeta.getMySQL2Hive(MySQL2HiveMeta.msl_table_temp)
  val writePartition = ValueUtils.getStringValue("spark.default.write.partition").toInt
  val partitionDoOrNot = mySQL2HiveMeta.hv_partition_doornot
  val partitionColumnsAdd = mySQL2HiveMeta.hv_partition_add_column.split(",")
  val partitionColumnsNotAdd = mySQL2HiveMeta.hv_partition_notadd_column.split(",")
  val partitionColumnsUnion = mySQL2HiveMeta.hv_partition_column_union
  val columnArr = ArrayBuffer[String]()
  columnArr ++= partitionColumnsAdd
  columnArr ++= partitionColumnsNotAdd
  val partitionColumnsChoose = mySQL2HiveMeta.hv_partition_choose
  val url = s"jdbc:mysql://${mySQL2HiveMeta.msl_host}:3306/${mySQL2HiveMeta.msl_dbase}?characterEncoding=utf-8"
  /**
    * Transfer: table and dbase
    *
    * @param spark
    * @param dbase
    * @param table
    * @param date
    */
  def mysql2hive(spark: SparkSession, dbase: String, table: String, date: String = ""): Unit = {
    var data: DataFrame = null
    data = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .option("user", mySQL2HiveMeta.msl_user)
      .option("password", mySQL2HiveMeta.msl_passwd)
      .option("inferSchema", "false")
      .load()

    val dbaseHive = mySQL2HiveMeta.hv_dbase
    val tableHive = mySQL2HiveMeta.hv_table
    /**
      * idea:  by sxwang :
      *
      * 表存不存在
      *    a.不存在 ->创建
      *    b.存在-> 比较schema
      *               a.相同 -> insert table
      *               b.不相同 -> alter table
      */
    tableExistsJudge(spark, dbaseHive, tableHive, data)
  }

  /** createNoPartitionTable:
    * 创建 没有分区的表
    *
    * @param dbase
    * @param table
    */
  def createNoPartitionTable(spark: SparkSession, data: DataFrame, dbase: String, table: String) = {
    data.coalesce(writePartition)
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .format("parquet")
      .saveAsTable(s"$dbase.$table")
     mysq2hivemeta(data,dbase,table,Array(""))
  }

  /**
    * 增加列
    *
    * @param data
    * @param size
    */
  def withCulomns(data: DataFrame, partitionColumns: Array[String], size: Int): DataFrame = {
    val timeFormat = FastDateFormat.getInstance("yyyy-MM-dd")
    var dataAddColumn: DataFrame = null
    size match {
      case 1 => dataAddColumn = data.withColumn(s"${partitionColumns(0)}", lit(timeFormat.format(new Date())))
    }
    dataAddColumn
  }

  /**insertMeta:
    *
    * @param partitions
    * @param partitionColumn
    * @param mysqltable
    * @param dbase
    * @param table
    * @param getPartitionExpression
    */
  def insertMeta(partitions:Array[Row],partitionColumn :Array[String],mysqltable: String, dbase: String, table: String, getPartitionExpression: String) = {
    var Expression = getPartitionExpression
    for(partition <- partitions){
      Expression= partitionColumn.indices.
        map(i => s"${partitionColumn(i)}='${partition(i).toString}'")
        .mkString(", ")
      DB.autoCommit{
        implicit session => {
          sql"insert into meta(mysqltable,hivetable,hivepartition) values(?,?,?)"
            .bind(mysqltable,s"$dbase.$table",Expression).update().apply()
        }
      }
    }
  }

  /**mysq2hivemeta:
    * etl 元数据记录
 *
    * @param dbase
    * @param table
    * @param partitionColumn
    */
  def mysq2hivemeta(data : DataFrame,dbase: String, table: String, partitionColumn: Array[String]) = {
    DBs.setupAll()
    val partitions = data.select(partitionColumn.head, partitionColumn.tail : _*).distinct().collect()
    val mysqltable = mySQL2HiveMeta.msl_dbase+"."+mySQL2HiveMeta.msl_table
    var getPartitionExpression=""

    partitionColumn.size match {
      case 0 => {
        DB.autoCommit{
          implicit session => {
            sql"insert into meta(mysqltable,hivetable,hivepartition) values(?,?,?)"
              .bind(mysqltable,s"$dbase.$table","null").update().apply()
          }
        }
      }
      case _ => insertMeta(partitions,partitionColumn,mysqltable,dbase,table,getPartitionExpression)
    }
    DBs.closeAll()
  }

  /** execute：
    * 执行建表
    *
    * @param data
    * @param dbase
    * @param table
    * @param partitionColumns
    */
  def executeCreateTable(data: DataFrame, dbase: String, table: String, partitionColumns: Array[String]) = {
    data.coalesce(writePartition).write.partitionBy(partitionColumns: _*)
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .format("parquet")
      .saveAsTable(s"$dbase.$table")

    partitionColumnsUnion match {
      case "true" => mysq2hivemeta(data, dbase, table, columnArr.toArray)
      case "false" => partitionColumnsChoose match {
        case "add" => mysq2hivemeta(data,dbase, table, partitionColumnsAdd)
        case "notadd" => mysq2hivemeta(data,dbase, table, partitionColumnsNotAdd)
      }
    }
  }

  /** execute：
    * 执行插入数据
    *
    * @param data
    * @param dbase
    * @param table
    * @param partitionColumns
    */
  def executeInserTable(data: DataFrame, dbase: String, table: String, partitionColumns: Array[String]) = {
    data.coalesce(writePartition).write.partitionBy(partitionColumns: _*)
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .format("parquet")
      .parquet(s"/user/hive/warehouse/$dbase.db/$table")
    partitionColumnsUnion match {
      case "true" => mysq2hivemeta(data, dbase, table, columnArr.toArray)
      case "false" => partitionColumnsChoose match {
        case "add" => mysq2hivemeta(data,dbase, table, partitionColumnsAdd)
        case "notadd" => mysq2hivemeta(data,dbase, table, partitionColumnsNotAdd)
      }
    }
  }

  /** createPartitionTable:
    * 创建一个分区表
    *
    * @param data
    * @param dbase
    * @param table
    */
  def createPartitionTable(spark: SparkSession, data: DataFrame, dbase: String, table: String) = {
    partitionColumnsUnion match {
      case "true" => executeCreateTable(data, dbase, table, columnArr.toArray)
      case "false" => partitionColumnsChoose match {
        case "add" => {
          var dataTem = withCulomns(data, partitionColumnsAdd, partitionColumnsAdd.size)
          executeCreateTable(dataTem, dbase, table, partitionColumnsAdd)
        }
        case "notadd" =>
          executeCreateTable(data, dbase, table, partitionColumnsNotAdd)
      }
    }
    spark.stop()
  }

  /** createTableIfNotExist：
    * 创建表
    *
    * @param spark
    * @param dbase
    * @param table
    */
  def createTableIfNotExist(spark: SparkSession, dbase: String, table: String, data: DataFrame): Unit = {
    partitionDoOrNot match {
      case "do" => createPartitionTable(spark, data, dbase, table)
      case "not" => createNoPartitionTable(spark, data, dbase, table)
    }
    spark.stop()
  }

  /** insertDoPartitionTable:
    * 插入分区表
    *
    * @param spark
    * @param data
    * @param dbase
    * @param table
    */
  def insertDoPartitionTable(spark: SparkSession, data: DataFrame, dbase: String, table: String) = {
    partitionColumnsUnion match {
      case "true" => executeInserTable(data, dbase, table, columnArr.toArray)
      case "false" => partitionColumnsChoose match {
        case "add" => {
          var dataTem = withCulomns(data, partitionColumnsAdd, partitionColumnsAdd.size)
          executeInserTable(dataTem, dbase, table, partitionColumnsAdd)
        }
        case "notadd" =>
          executeInserTable(data, dbase, table, partitionColumnsNotAdd)
      }
    }
  }

  /** insertNotPartitionTable：
    * 插入不带分区表
    *
    * @param spark
    * @param data
    * @param dbase
    * @param table
    */
  def insertNotPartitionTable(spark: SparkSession, data: DataFrame, dbase: String, table: String) = {
    data.coalesce(writePartition)
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .parquet(s"/user/hive/warehouse/$dbase.db/$table")
    mysq2hivemeta(data,dbase,table,Array(""))
  }

  /**
    * insertHiveTable:
    * data 与  hive表里的 schema 相同 不用更新表的schema
    *
    * @param spark
    * @param dbase
    * @param table
    * @param data
    */
  def insertHiveTable(spark: SparkSession, dbase: String, table: String, data: DataFrame): Unit = {
    partitionDoOrNot match {
      case "do" => insertDoPartitionTable(spark, data, dbase, table)
      case "not" => insertNotPartitionTable(spark, data, dbase, table)
    }
    spark.stop()
  }

  /**convertToLegalHdfsPath:
    * 转换hdfs路径
    * @param path
    */
  def convertToLegalHdfsPath(path: String) = {
    path.replace(":", "%3A")
  }

  /**getHdfsPath：
    * 获取表hdfs路径
    *
    * @param dbase
    * @param table
    * @param partition
    * @param partitionColumn
    */
  def getHdfsPath(dbase: String, table: String, partition: Row, partitionColumn: Array[String]) = {
    convertToLegalHdfsPath(s"/user/hive/warehouse/$dbase.db/$table/" +
      partitionColumn.indices.map(i => s"${partitionColumn(i)}=${partition(i).toString}").mkString("/"))
  }

  /**deleteOldPartitionDir：
    * 删除添加列之前的分区路径
 *
    * @param spark
    * @param dbase
    * @param table
    * @param partition
    * @param partitionColumn
    */
  def deleteOldPartitionDir(spark: SparkSession, dbase: String, table: String, partition: Row, partitionColumn: Array[String]) = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val pathStr = getHdfsPath(dbase, table, partition, partitionColumn)
    val path = new Path(pathStr)
    if (hdfs.exists(path)) {
      hdfs.delete(path, true)
    }
  }

  /**dropOldPartition：
    * 删除旧的分区
 *
    * @param spark
    * @param dbase
    * @param table
    * @param data
    */
  def dropOldPartition(spark: SparkSession, dbase: String, table: String, data: DataFrame,partitionColumn:Array[String]) = {
    val partitions = data.select(partitionColumn.head, partitionColumn.tail : _*).distinct().collect()
    for(partition <- partitions){
      val getPartitionExpression = partitionColumn.indices.
        map(i => s"${partitionColumn(i)}='${partition(i).toString}'")
        .mkString(", ")
      val sql = s"alter table $dbase.$table drop partition($getPartitionExpression)"
      println("====================drop partition：==================="+sql+"=======================================")
      spark.sql(sql)
      deleteOldPartitionDir(spark, dbase, table, partition,partitionColumn)
    }

  }

  /**addPartition：
    * 添加分区
    * @param spark
    * @param dbase
    * @param table
    * @param data
    * @param partitionColumn
    */
  def addPartition(spark: SparkSession, dbase: String, table: String, data: DataFrame, partitionColumn: Array[String]) = {
    val partitions = data.select(partitionColumn.head, partitionColumn.tail : _*).distinct().collect()
    for(partition <- partitions){
      val getPartitionExpression = partitionColumn.indices.
        map(i => s"${partitionColumn(i)}='${partition(i).toString}'")
        .mkString(", ")
      val sql = s"alter table $dbase.$table add partition($getPartitionExpression) " +
        s"location '${getHdfsPath(dbase, table, partition, partitionColumn)}'"
      spark.sql(sql)
    }
  }

  /** updateHiveTable：
    * data 与  hive表里的 schema 不相同 更新表的schema
    *
    * @param spark
    * @param dbase
    * @param table
    * @param data
    */
  def updateHiveTable(spark: SparkSession, dbase: String, table: String, data: DataFrame, newaddColumns: Seq[String]): Unit = {
    /**
      * 第一种方法报错：sxwang 无法解决
      */
    //    spark.sql(s"alter table $dbase.$table add columns(${newaddColumns.map(c => s"$c string").mkString(", ")})")
//    partitionColumnsUnion match {
//      case "true" => dropOldPartition(spark, dbase, table, data,columnArr.toArray)
//      case "false" => partitionColumnsChoose match {
//        case "add" => dropOldPartition(spark, dbase, table, data,partitionColumnsAdd)
//        case "notadd" => dropOldPartition(spark, dbase, table, data,partitionColumnsNotAdd)
//      }
//    }
//    partitionColumnsUnion match {
//      case "true" => addPartition(spark, dbase, table, data,columnArr.toArray)
//      case "false" => partitionColumnsChoose match {
//        case "add" => addPartition(spark, dbase, table, data,partitionColumnsAdd)
//        case "notadd" => addPartition(spark, dbase, table, data,partitionColumnsNotAdd)
//      }
//    }
//    spark.stop()

    /**
      * 第二种临时办法：sxwang
      */
    spark.sql(s"drop table $dbase.$table ")
    createTableIfNotExist(spark, dbase, table, data)
  }

  /**
    * contrastSchemaWithExist:
    * 比较schema：
    *       a.相同 -> insert table
    *       b.不相同 -> alter table
    *
    * @param spark
    * @param dbase
    * @param table
    */
  def contrastSchemaWithExist(spark: SparkSession, dbase: String, table: String, data: DataFrame): Unit = {
    val dataColumns = data.schema.map(_.name)
    val hiveTargetColumns = spark.table(s"$dbase.$table").schema.map(_.name)
    val judgeUpdateRes = dataColumns.filter(!hiveTargetColumns.contains(_))
    judgeUpdateRes.length match {
      case 0 => insertHiveTable(spark, dbase, table, data)
      case _ => updateHiveTable(spark, dbase, table, data, judgeUpdateRes)
    }
  }

  /**
    * table judge exist
    *
    * @param dbase
    * @param table
    */
  def tableExistsJudge(spark: SparkSession, dbase: String, table: String, data: DataFrame): Unit = {
    import spark.implicits._
    val catalog = spark.catalog
    val judgeRes = catalog.listTables(dbase).map(_.name).filter(c => c.equals(table)).count()
    judgeRes match {
      case 0 => createTableIfNotExist(spark, dbase, table, data)
      case _ => contrastSchemaWithExist(spark, dbase, table, data)
    }
  }
}
