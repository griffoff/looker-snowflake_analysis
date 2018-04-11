view: warehouse_usage {
  label: "Warehouse Usage"
  #sql_table_name: ZPG.WAREHOUSE_USAGE_DETAIL ;;
  derived_table: {
    sql:
      select
        wu.warehouse_name
        ,coalesce(wud.start_time, wu.start_time) as start_time
        ,wu.credits_used as total_credits_used
        ,wud.query_id
        ,wud.database_name
        ,wud.schema_name
        ,wud.query_type
        ,wud.user_name
        ,wud.role_name
        ,wud.warehouse_size
        ,wud.total_elapsed_time as total_elapsed_time_ms
        ,case when query_type not in ('DROP_CONSTRAINT', 'ALTER_TABLE_MODIFY_COLUMN', 'ALTER_TABLE_ADD_COLUMN', 'ALTER_TABLE_DROP_COLUMN', 'RENAME_COLUMN', 'DESCRIBE', 'SHOW', 'CREATE_TABLE', 'ALTER_SESSION', 'USE', 'DROP', 'CREATE CONSTRAINT', 'ALTER USER')
              then total_elapsed_time_ms
              when query_id is not null
              then 0
              end as total_elapsed_time_credit_use_ms
        ,wud.query_text
        ,total_elapsed_time_credit_use_ms / nullif(sum(total_elapsed_time_credit_use_ms) over (partition by wu.start_time, wu.warehouse_name), 0) as credits_used_percent
        ,coalesce(credits_used_percent, 1) * total_credits_used as credits_used
        ,row_number() over (order by (wud.start_time, wu.start_time), wu.warehouse_name) as id
    from ZPG.WAREHOUSE_USAGE wu
    left join ZPG.WAREHOUSE_USAGE_DETAIL wud on wu.warehouse_name = wud.warehouse_name
                                            and wu.start_time = wud.start_hour;;
    sql_trigger_value: select count(*) from ZPG.WAREHOUSE_USAGE_DETAIL  ;;
  }

  set: query_details {
    fields: [start_time, query_text, query_type, warehouse_name, database_name, schema_name, role_name, user_name, total_elapsed_time, warehouse_cost]
  }
  dimension: database_name {
    type: string
    sql: ${TABLE}.DATABASE_NAME ;;
  }

  dimension: id {
    type: number
    primary_key: yes
    hidden: yes
  }

  dimension: query_id {
    type: string
    sql: ${TABLE}.QUERY_ID ;;
  }

  dimension: query_text {
    type: string
    sql: ${TABLE}.QUERY_TEXT ;;
  }

  dimension: query_type {
    type: string
    sql: ${TABLE}.QUERY_TYPE ;;
  }

  dimension: role_name {
    type: string
    sql: ${TABLE}.ROLE_NAME ;;
  }

  dimension: schema_name {
    type: string
    sql: ${TABLE}.SCHEMA_NAME ;;
  }

  dimension_group: start {
    label: "Query"
    type: time
    timeframes: [
      raw,
      time,
      hour_of_day,
      hour,
      hour3,
      hour6,
      date,
      day_of_week,
      day_of_month,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.START_TIME ;;
  }

  dimension: user_name {
    type: string
    sql: ${TABLE}.USER_NAME ;;
  }

  dimension: warehouse_name {
    type: string
    sql: ${TABLE}.WAREHOUSE_NAME ;;
    hidden: no
  }

  dimension: warehouse_size {
    type: string
    sql: ${TABLE}.WAREHOUSE_SIZE ;;
  }

  measure: count {
    type: count
    drill_fields: [query_details*]
  }

  measure: total_elapsed_time {
    type: sum
    sql: ${TABLE}.TOTAL_ELAPSED_TIME_MS / 3600 / 24 ;;
    value_format_name: duration_hms
    drill_fields: [query_details*]
  }

  measure: total_elapsed_time_credit_use {
    type: sum
    sql: ${TABLE}.total_elapsed_time_credit_use_ms / 2600 / 24 ;;
    value_format_name: duration_hms
    drill_fields: [query_details*]
  }

  measure: credits_used_percent {
    type: sum
    value_format_name: percent_2
  }

  measure: credits_used {
    type: sum
    value_format_name: decimal_2
    drill_fields: [query_details*]
  }

  measure: warehouse_cost_per_credit {
    type: number
    sql: 1.4 ;;
    hidden: yes
  }

  measure: warehouse_cost {
    label:"Warehouse Cost"
    type: number
    sql: ${credits_used} * ${warehouse_cost_per_credit} ;;
    value_format_name: currency
    drill_fields: [query_details*]
  }

  measure: warehouse_cost_monthly {
    label:"Warehouse Cost (1 month at this rate)"
    type: number
    sql: ${warehouse_cost} * 365 / 12;;
    value_format_name: currency
    drill_fields: [query_details*]
    required_fields: [start_date]
  }

  measure: warehouse_cost_monthly_day_2 {
    label:"Warehouse Cost (1 month at the last 7 days avg daily rate)"
    type: number
    sql: sum(${warehouse_cost}) over (order by ${start_date} rows between 7 preceding and current row) * 365 / 12 / 7 ;;
    value_format_name: currency
    required_fields: [start_date]
  }

  measure: warehouse_cost_monthly_6 {
    label:"Warehouse Cost (1 month at the last 6 hours avg rate)"
    type: number
    sql: sum(${warehouse_cost}) over (order by ${start_hour} rows between 6 preceding and current row) * 4 * 365 / 12;;
    value_format_name: currency
    drill_fields: [query_details*]
    required_fields: [start_hour]
  }

  measure: warehouse_cost_mtd {
    required_fields: [start_date, start_month]
    type: number
    sql: sum(${warehouse_cost}) over (partition by ${start_month} order by ${start_date} rows unbounded preceding) ;;
    value_format_name: currency
  }

}
