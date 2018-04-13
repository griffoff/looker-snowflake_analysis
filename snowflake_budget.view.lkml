view: snowflake_budget {
  sql_table_name: UPLOADS.BUDGETS.SNOWFLAKE_BUDGET ;;

  dimension: _fivetran_deleted {
    type: yesno
    sql: ${TABLE}._FIVETRAN_DELETED ;;
    hidden: yes
  }

  dimension: _fivetran_synced {
    type: string
    sql: ${TABLE}._FIVETRAN_SYNCED ;;
    hidden: yes
  }

  dimension: _row {
    type: number
    sql: ${TABLE}._ROW ;;
    hidden: yes
  }

  dimension: growth_reason {
    type: string
    sql: ${TABLE}.GROWTH_REASON ;;
  }

  dimension: month {
    type: date_month
    sql: to_date(${TABLE}.MONTH, 'MON-YY') ;;
    primary_key: yes
  }

  dimension: processing_node_hours {
    type: string
    sql: ${TABLE}.PROCESSING_NODE_HOURS ;;
    hidden: yes
  }

  dimension: seasonal_fluctuation {
    type: string
    sql: ${TABLE}.SEASONAL_FLUCTUATION ;;
    hidden: yes
  }

  measure: snowflake_with_growth {
    label: "Monthly Budget"
    type: sum
    sql: to_decimal(${TABLE}.SNOWFLAKE_WITH_GROWTH, '\$9,999,990') ;;
    value_format_name: currency
  }

  dimension: storage_processing_growth {
    type: string
    sql: ${TABLE}.STORAGE_PROCESSING_GROWTH ;;
    hidden: yes
  }

}
