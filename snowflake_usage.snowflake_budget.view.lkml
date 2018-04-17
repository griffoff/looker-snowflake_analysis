view: snowflake_budget {
  label: "Budget"
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

  dimension: month_raw {
    type: date_raw
    sql: to_date(${TABLE}.MONTH, 'MON-YY');;
    primary_key: yes
    hidden: yes
  }

  dimension: month {
    type: date_month
    sql: ${month_raw} ;;
    hidden: yes
  }

  dimension_group: budget {
    type: time
    timeframes: [month, year, fiscal_month_num, fiscal_year]
    sql: ${month_raw};;
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
    value_format_name: usd_0
  }

  measure: invoiced {
    label: "Invoiced Amount"
    type: sum
    sql: to_decimal(${TABLE}.Invoiced, '\$9,999,990') ;;
    value_format_name: usd_0
  }

  dimension: storage_processing_growth {
    type: string
    sql: ${TABLE}.STORAGE_PROCESSING_GROWTH ;;
    hidden: yes
  }

}
