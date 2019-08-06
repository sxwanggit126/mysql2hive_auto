package meta

case  class Meta(
                  msl_dbase :String,
                  msl_table :String,
                  msl_user :String,
                  msl_passwd :String,
                  msl_host :String,
                  hv_dbase :String,
                  hv_table :String,
                  hv_partition_doornot :String,
                  hv_partition_choose :String,
                  hv_partition_notadd_column :String,
                  hv_partition_add_column :String,
                  hv_partition_column_union :String
                )
