package app

import meta.Meta
import scalikejdbc.DB
import scalikejdbc.config.DBs
import scalikejdbc._
object MySQL2HiveMeta {
  var msl_table_temp =""
  def getMySQL2Hive(msl_table: String) = {
    //读取
    DBs.setup()

    val mysql2hivemeta: List[Meta] = DB.readOnly { implicit session => {
      sql"select * from meta_mysql2hive  where msl_table = $msl_table".map(rs => {
        Meta(
          rs.string("msl_dbase"),
          rs.string("msl_table"),
          rs.string("msl_user"),
          rs.string("msl_passwd"),
          rs.string("msl_host"),
          rs.string("hv_dbase"),
          rs.string("hv_table"),
          rs.string("hv_partition_doornot"),
          rs.string("hv_partition_choose"),
          rs.string("hv_partition_notadd_column"),
          rs.string("hv_partition_add_column"),
          rs.string("hv_partition_column_union")
        )
      }).list().apply()
    }
    }
    DBs.closeAll()
    var meta : Meta = null
    mysql2hivemeta.foreach(r => {
      meta = Meta(
        r.msl_dbase,
        r.msl_table,
        r.msl_user,
        r.msl_passwd,
        r.msl_host,
        r.hv_dbase,
        r.hv_table,
        r.hv_partition_doornot,
        r.hv_partition_choose,
        r.hv_partition_notadd_column,
        r.hv_partition_add_column,
        r.hv_partition_column_union
      )
    })
    println(meta)
    meta
  }

}
