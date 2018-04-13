# If necessary, uncomment the line below to include explore_source.

# include: "snowflake_analysis.model.lkml"

view: warehouse_usage_monthly {
  label: "Compute Costs"
  derived_table: {
    explore_source: warehouse_usage {
      column: warehouse_cost {}
      column: start_month {}
    }
  }
  measure: warehouse_cost {
    value_format_name: usd_0
    type: sum
  }
  dimension: start_month {
    type: date_raw
    hidden: yes
  }
}
