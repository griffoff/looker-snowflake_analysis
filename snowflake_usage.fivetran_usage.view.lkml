# If necessary, uncomment the line below to include explore_source.

# include: "snowflake_analysis.model.lkml"

view: fivetran_usage {
  derived_table: {
    explore_source: warehouse_usage {
      column: warehouse_cost {}
      column: warehouse_name {}
      column: database_name {}
      column: usage_date {field:warehouse_usage.start_date}
      filters: {
        field: warehouse_usage.user_name
        value: "FIVETRAN%"
      }
    }
  }
  measure: warehouse_cost {
    label: "Processing/Sync Cost"
    value_format_name: usd
    type: sum
  }
  dimension: warehouse_name {}
  dimension: database_name {}
  dimension_group: usage {
    label: "Usage Date"
    type: time
    timeframes: [raw, date, month, week, year, day_of_week]
    sql: ${TABLE}.usage_date ;;
  }
  measure: total_cost {
    value_format_name: usd
    type: number
    sql: ${warehouse_cost} + ${database_storage.storage_cost} ;;
    required_fields: [database_storage.storage_cost]
  }
}
