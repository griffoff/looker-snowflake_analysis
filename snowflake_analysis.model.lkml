connection: "snowflake_dev"

include: "*.view"         # include all views in this project
include: "*.dashboard.lookml"  # include all dashboards in this project
include: "/core/common.lkml"

case_sensitive: no

explore: snowflake_budget {
  join: database_storage_monthly {
    sql_on: ${snowflake_budget.month} = ${database_storage_monthly.usage_month} ;;
    relationship: one_to_one
  }
  join: warehouse_usage_monthly {
    sql_on: ${snowflake_budget.month} = ${warehouse_usage_monthly.start_month} ;;
    relationship: one_to_one
  }
}

explore: warehouse_usage  {
  fields: [ALL_FIELDS*]
  join: database_storage {
    #fields: [database_storage.storage_cost]
    sql_on: ${warehouse_usage.start_date} = ${database_storage.usage_date} ;;
    relationship: many_to_many
  }
  join: snowflake_budget {
    sql_on: ${warehouse_usage.start_month} = ${snowflake_budget.month} ;;
    relationship: many_to_one
  }
}

explore: database_storage {
  from: database_storage
  view_name: database_storage
  join: warehouse_usage {
    #fields: [warehouse_usage.warehouse_cost]
    sql_on: ${database_storage.usage_date} = ${warehouse_usage.start_date}
          --and ${database_storage.database_name} = ${warehouse_usage.database_name}
          ;;
    #type: full_outer
      relationship: many_to_many
    }
  join: snowflake_budget {
    sql_on: ${database_storage.usage_month} = ${snowflake_budget.month} ;;
    relationship: many_to_one
  }
}
