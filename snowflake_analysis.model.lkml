connection: "snowflake_dev"

include: "*.view"         # include all views in this project
include: "*.dashboard.lookml"  # include all dashboards in this project
include: "/core/common.lkml"

case_sensitive: no

explore: warehouse_usage  {
  fields: [ALL_FIELDS*]
  join: database_storage {
    fields: [database_storage.storage_cost]
    sql_on: ${warehouse_usage.start_date} = ${database_storage.usage_date} ;;
    relationship: many_to_many
  }
}

explore: database_storage {
  from: database_storage
  view_name: database_storage
  join: warehouse_usage_detail {
    fields: [warehouse_usage_detail.warehouse_cost]
    from:warehouse_usage
    sql_on: ${database_storage.usage_date} = ${warehouse_usage_detail.start_date}
          --and ${database_storage.database_name} = ${warehouse_usage_detail.database_name}
          ;;
    #type: full_outer
      relationship: many_to_many
    }
  }
