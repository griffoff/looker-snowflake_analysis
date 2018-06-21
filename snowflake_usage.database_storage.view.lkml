view: database_storage {
  #sql_table_name: ZPG.DATABASE_STORAGE ;;
  derived_table: {
    sql:
      with daily as (
        select
            *
            ,(average_database_bytes + average_failsafe_bytes) / power(1024, 4) as db_tb
            ,sum(db_tb) over (partition by usage_date) as total_tb
        FROM USAGE.SNOWFLAKE.DATABASE_STORAGE
        )
        select
            *
            ,extract(day FROM least(max(usage_date) over (), LAST_DAY(usage_date))) as days_in_month
            ,avg(total_tb) over (partition by date_trunc(month, usage_date)) / days_in_month as monthly_total_tb
            ,avg(db_tb) over (partition by database_name, date_trunc(month, usage_date)) / days_in_month as monthly_db_tb
        from daily
      ;;
    sql_trigger_value: select count(*) from zpg.database_storage ;;
  }

  dimension: pk {
    sql: ${database_name} || ${usage_date} ;;
    hidden: yes
    primary_key: yes
  }
  dimension: average_database_bytes {
    type: number
    sql: ${TABLE}.AVERAGE_DATABASE_BYTES ;;
    hidden: yes
  }

  dimension: average_failsafe_bytes {
    type: number
    sql: ${TABLE}.AVERAGE_FAILSAFE_BYTES ;;
    hidden: yes
  }

  measure: monthly_db_tb {
    label: "Total DB Size"
    description: "Monthly DB Size for cost calculations"
    type: sum
    value_format_name: TB_1
  }

  measure: db_tb {
    label: "Avg. DB Size"
    type: average
    sql: ${TABLE}.db_tb * 1024 ;;
    value_format_name: MB
  }

  measure: db_count {
    label: "# DBs"
    type: count_distinct
    sql: ${database_name} ;;
  }

  measure: credit_usage_value {
    type: number
    sql: ${monthly_db_tb}
      ;;
    value_format_name: TB_1
    hidden: yes
  }

  measure: credit_usage {
    type: number
    sql: ${credit_usage_value} ;;
    value_format_name: TB_1
  }

  dimension: storage_rate {
    type: number
    sql: 23;;
    hidden: yes
  }

  dimension_group: usage {
    type: time
    timeframes: [
      raw,
      hour,
      date,
      week,
      month,
      year,
      fiscal_quarter,
      fiscal_year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.USAGE_DATE ;;
  }

  dimension: component_type {
    type: string
    sql: 'Storage' ;;
  }

  dimension: component {
    type: string
    sql: ${database_name} ;;
  }

  measure: component_cost {
    type: number
    sql: ${storage_cost} ;;
    value_format_name: usd
  }

  measure: storage_cost {
    type: number
    sql:  ${credit_usage} * ${storage_rate} ;;
    value_format_name: usd
  }

  dimension: database_name {
    type: string
    sql: ${TABLE}.DATABASE_NAME ;;
  }


}
