# If necessary, uncomment the line below to include explore_source.

# include: "snowflake_analysis.model.lkml"

view: fivetran_usage {
  derived_table: {
    explore_source: warehouse_usage {
      column: warehouse_cost {}
      column: warehouse_name {}
      column: database_name {}
      column: usage_date {field:warehouse_usage.start_time}
      filters: {
        field: warehouse_usage.user_name
        value: "FIVETRAN%"
      }
    }
  }
  measure: warehouse_cost {
    value_format_name: usd
    type: sum
  }
  dimension: warehouse_name {}
  dimension: database_name {}
  dimension_group: usage {
    label: "Usage Date"
    type: time
    timeframes: [raw, date, month, year, hour_of_day]
    sql: ${TABLE}.usage_date ;;
  }
}
