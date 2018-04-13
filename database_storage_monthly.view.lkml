# If necessary, uncomment the line below to include explore_source.

# include: "snowflake_analysis.model.lkml"

view: database_storage_monthly {
  label: "Storage Costs"
  derived_table: {
    explore_source: database_storage {
      column: usage_month {}
      column: monthly_storage_cost {}
    }
  }

  dimension: usage_month {
    type:date_raw
    hidden: yes
  }

  measure: total_storage_cost {
    type: sum
    sql: ${TABLE}.monthly_storage_cost ;;
    value_format_name: usd_0
  }
}
