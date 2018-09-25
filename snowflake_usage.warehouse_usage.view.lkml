view: warehouse_usage {
  label: "Warehouse Usage"
  #sql_table_name: ZPG.WAREHOUSE_USAGE_DETAIL ;;
  derived_table: {
    create_process: {
      sql_step:
        CREATE TABLE IF NOT EXISTS ${SQL_TABLE_NAME} (
              START_TIME TIMESTAMP_LTZ(9),
              QUERY_START_TIME TIMESTAMP_LTZ(9),
              WAREHOUSE_NAME STRING,
              TOTAL_CREDITS_USED NUMBER(38,12),
              QUERY_ID STRING,
              QUERY_TYPE STRING,
              QUERY_TEXT STRING,
              QUERY_TAG STRING,
              USER_NAME STRING,
              ROLE_NAME STRING,
              DATABASE_NAME STRING,
              SCHEMA_NAME STRING,
              WAREHOUSE_SIZE STRING,
              EXECUTION_STATUS STRING,
              ERROR_MESSAGE STRING,
              ELAPSED_TIME_MS NUMBER(10,0),
              ELAPSED_TIME_CREDIT_USE_MS NUMBER(10,0),
              TOTAL_ELAPSED_TIME_CREDIT_USE_MS NUMBER(10,0),
              CREDITS_USED_PERCENT NUMBER(11,10),
              CREDITS_USED NUMBER(18,12),
              ID NUMBER(18,0)
            ) ;;
      sql_step:
        create sequence if not exists looker_scratch.warehouse_usage_id
        ;;
      sql_step:
        create table if not exists looker_scratch.warehouse_usage
        as
        select *
        from snowflake.account_usage.warehouse_metering_history
        ;;
      sql_step:
        create or replace temporary table looker_scratch.wu_new
        as
        select *
        from snowflake.account_usage.warehouse_metering_history
        where start_time > (select max(start_time) from looker_scratch.warehouse_usage)
        ;;
      sql_step:
        insert into looker_scratch.warehouse_usage
        select * from looker_scratch.wu_new
        ;;
      sql_step:
        create table if not exists looker_scratch.warehouse_usage_detail
        as
        select *
        from snowflake.account_usage.query_history
        where execution_status != 'running'
        ;;
      sql_step:
        create or replace temporary table looker_scratch.wud_new
        as
        select *
        from snowflake.account_usage.query_history
        where execution_status != 'running'
        and start_time > (select max(start_time) from looker_scratch.warehouse_usage_detail)
        ;;
      sql_step:
        delete from looker_scratch.warehouse_usage_detail
        where query_id in (select query_id from looker_scratch.wud_new)
        ;;
      sql_step:
        insert into looker_scratch.warehouse_usage_detail
        select * from looker_scratch.wud_new
        ;;
      sql_step:
        delete from ${SQL_TABLE_NAME}
        where start_time >= dateadd(day, -1, current_date());;
      sql_step:
        set latest = (select max(start_time) from ${SQL_TABLE_NAME});;
      sql_step:
        insert into ${SQL_TABLE_NAME}
        select
          --coalesce(wud.start_time, wu.start_time) as start_time
          wu.start_time
          ,wud.start_time as query_start_time
          ,wu.warehouse_name, wu.credits_used as total_credits_used
          ,wud.query_id, wud.query_type, wud.query_text, wud.query_tag, wud.user_name, wud.role_name, wud.database_name, wud.schema_name, wud.warehouse_size
          ,wud.execution_status, wud.error_message
          ,case
              when wud.end_time > wu.end_time --query runs over time boundary, this is the portion of the query BEFORE the boundary
              then date_part(epoch_millisecond, wu.end_time) - date_part(epoch_millisecond, wud.start_time) -- time boundary minus query start
              when wud.start_time < wu.start_time --query runs over time boundary, this is the portion of the query AFTER the boundary
              then date_part(epoch_millisecond, wud.end_time) - date_part(epoch_millisecond, wu.start_time) -- query end minus time boundary
              else total_elapsed_time
              end as elapsed_time_ms
          ,case
              when query_type not in ('DROP_CONSTRAINT', 'ALTER_TABLE_MODIFY_COLUMN', 'ALTER_TABLE_ADD_COLUMN', 'ALTER_TABLE_DROP_COLUMN', 'RENAME_COLUMN', 'DESCRIBE', 'SHOW', 'CREATE_TABLE', 'ALTER_SESSION', 'USE', 'DROP', 'CREATE CONSTRAINT', 'ALTER USER')
              then elapsed_time_ms
              when query_id is not null
              then 0
           end as elapsed_time_credit_use_ms
          ,sum(elapsed_time_credit_use_ms) over (partition by wu.start_time, wu.warehouse_name) as total_elapsed_time_credit_use_ms
          ,(elapsed_time_credit_use_ms / nullif(total_elapsed_time_credit_use_ms, 0))::decimal(20, 19) as credits_used_percent
          ,wu.credits_used * credits_used_percent as credits_used
          ,looker_scratch.warehouse_usage_id.nextval as id
        from looker_scratch.WAREHOUSE_USAGE wu
        left join looker_scratch.WAREHOUSE_USAGE_DETAIL wud on wu.warehouse_name = wud.warehouse_name
                      and (
                          wud.start_time between wu.start_time and wu.end_time
                          or wud.end_time between wu.start_time and wu.end_time --query may be in more than one hour bucket
                        )
        where wu.start_time > $latest
        or $latest is null
      ;;
    }
    sql_trigger_value: select count(*) from USAGE.SNOWFLAKE.WAREHOUSE_USAGE_DETAIL  ;;
  }

  set: query_details {
    fields: [start_time, query_text, query_type, warehouse_name, database_name, schema_name, role_name, user_name, total_elapsed_time_detail, warehouse_cost]
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
    group_label: "Query details"
    type: string
    sql: ${TABLE}.QUERY_ID ;;
  }

  dimension: query_text {
    group_label: "Query details"
    type: string
    sql: ${TABLE}.QUERY_TEXT ;;
  }

  dimension: query_type {
    group_label: "Query details"
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

  dimension: usage_date {
    type: date_raw
    sql: ${start_raw}::date ;;
    hidden: yes
  }

  dimension_group: start {
    label: "Query"
    type: time
    timeframes: [
      raw,
      time,
      hour_of_day,
      time_of_day,
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
    sql: COALESCE(${TABLE}.QUERY_START_TIME, ${TABLE}.START_TIME) ;;
  }

  measure: latest_start_time {
    label: "Up to date as of:"
    type: date_time
    sql: max(${start_raw}) ;;
  }

  measure: data_recency {
    label: "Age of data"
    type: number
    sql: datediff(second, ${latest_start_time}, current_timestamp()) / 3600 / 24 ;;
    value_format: "d \d\a\y\s h \h\r\s m \m\i\n\s"
  }

  dimension: user_name {
    type: string
    sql: ${TABLE}.USER_NAME ;;
  }

  dimension: user_type {
    type: string
    case: {
      when: {
        sql: ${user_name} is null ;;
        label: "Unknown"
        # warehouse usage data captured before detail was being captured
      }
      when: {
        sql: ${user_name} ilike 'FIVETRAN%' ;;
        label: "Data Sync - FiveTran"
      }
      when: {
        sql: ${user_name} ilike 'REALTIME_SYNC%' ;;
        label: "Data Sync - RealTime"
      }
      when: {
        sql:  ${user_name} ilike 'LOOKER%' ;;
        label: "Service - Looker"
      }
      when: {
        sql:  ${user_name} ilike 'LO_APP' ;;
        label: "Service - Learning Objects"
      }
      when: {
        sql:  ${user_name} ilike 'AIRFLOW%' or ${user_name} ilike 'SVCPA';;
        label: "Service - Airflow"
      }
      when: {
        sql:  ${user_name} ilike 'BNOC%' ;;
        label: "Service - BNOC"
      }
      when: {
        sql:  ${user_name} ilike 'CAP_ER%' ;;
        label: "Service - CAP/ER"
      }
      when: {
        sql:  ${user_name} ilike 'UNLIMITED_SPARK%' ;;
        label: "Service - CAP/UNLIMITED"
      }
      else: "User"
    }
  }

  dimension: warehouse_name {
    type: string
    sql: ${TABLE}.WAREHOUSE_NAME ;;
  }

  dimension: warehouse_size {
    type: string
    sql: ${TABLE}.WAREHOUSE_SIZE ;;
  }

  dimension: component_type {
    type: string
    sql: 'Compute' ;;
  }

  dimension: component {
    type: string
    sql: ${warehouse_name} ;;
  }

  measure: component_cost {
    type: number
    sql: ${warehouse_cost} ;;
    value_format_name: usd
  }

  measure: count {
    label: "# Queries"
    type: count
    drill_fields: [query_details*]
  }

  dimension: elapsed_time {
    type: number
    sql: ${TABLE}.TOTAL_ELAPSED_TIME_MS / 1000 / 3600 / 24 ;;
    hidden: yes
  }

  measure: total_elapsed_time_detail {
    label: "Query Time"
    group_label: "Query Time"
    type: sum
    sql: ${elapsed_time} ;;
    value_format_name: duration_hms
    hidden: yes
  }

  dimension: elapsed_time_buckets {
    label: "Query Time (buckets)"
    type: tier
    style: relational
    tiers: [0.00001157407, 0.00002314814, 0.00004629629, 0.00009259259, 0.00018518518, 0.00037037037]
    sql: ${elapsed_time} ;;
    value_format_name: duration_hms

  }

  measure: total_elapsed_time {
    label: "Query Time (Total)"
    group_label: "Query Duration"
    type: sum
    sql: ${elapsed_time} ;;
    value_format_name: duration_dhm
    drill_fields: [query_details*]
  }

  measure: cost_per_query {
    type: number
    sql: ${warehouse_cost} / ${number_of_chargable_queries} ;;
    value_format_name: usd
  }

  measure: number_of_chargable_queries {
    label: "# Chargable Queries"
    type: number
    sql: count(case when ${TABLE}.total_elapsed_time_credit_use_ms > 0 then 1 end) ;;
  }

  measure: avg_elapsed_time {
    label: "Query Time (Average)"
    group_label: "Query Duration"
    type: average
    sql: ${elapsed_time} ;;
    value_format_name: duration_hms
    drill_fields: [query_details*]
  }

  measure: max_elapsed_time {
    label: "Query Time (Max)"
    group_label: "Query Duration"
    type: max
    sql: ${elapsed_time} ;;
    value_format_name: duration_hms
    drill_fields: [query_details*]
  }

  measure: min_elapsed_time {
    label: "Query Time (Min)"
    group_label: "Query Duration"
    type: min
    sql: ${elapsed_time} ;;
    value_format_name: duration_hms
    drill_fields: [query_details*]
  }

  measure: med_elapsed_time {
    label: "Query Time (Median)"
    group_label: "Query Duration"
    type: median
    sql: ${elapsed_time} ;;
    value_format_name: duration_hms
    drill_fields: [query_details*]
  }

  measure: stdev_elapsed_time {
    label: "Query Time (Std. Dev.)"
    group_label: "Query Duration"
    type: number
    sql: stddev(${elapsed_time}) ;;
    value_format_name: duration_hms
    drill_fields: [query_details*]
  }

  measure: total_elapsed_time_credit_use {
    label: "Total Chargable Query Time"
    type: sum
    sql: ${TABLE}.total_elapsed_time_credit_use_ms / 3600 / 24 ;;
    value_format_name: duration_hms
    drill_fields: [query_details*]
  }

  measure: credits_used_percent {
    type: sum
    value_format_name: percent_2
    hidden: yes
  }

  measure: credits_used {
    type: sum
    value_format_name: decimal_2
    drill_fields: [query_details*]
    hidden: yes
  }

  measure: warehouse_cost_per_credit {
    type: number
    sql: 1.4 ;;
    hidden: yes
  }

  measure: warehouse_cost {
    label:"Warehouse Cost"
    type: number
    sql: case when ${credits_used} >= 0 then ${credits_used} end * ${warehouse_cost_per_credit};;
    value_format_name: usd
    drill_fields: [query_details*]
  }

  measure: warehouse_cost_avg {
    label:"Warehouse Cost (Avg)"
    type: number
    sql: ${warehouse_cost} / ${count};;
    value_format_name: usd
    drill_fields: [query_details*]
  }

  measure: warehouse_cost_monthly {
    label:"Warehouse Cost (1 month at this rate)"
    type: number
    sql: ${warehouse_cost} * 365 / 12;;
    value_format_name: usd
    drill_fields: [query_details*]
    required_fields: [start_date]
  }

  measure: warehouse_cost_monthly_day_2 {
    label:"Warehouse Cost (1 month at the last 7 days avg daily rate)"
    type: number
    sql: sum(${warehouse_cost}) over (order by ${start_date} rows between 7 preceding and current row) * 365 / 12 / 7 ;;
    value_format_name: usd
    required_fields: [start_date]
  }

  measure: warehouse_cost_monthly_6 {
    label:"Warehouse Cost (1 month at the last 6 hours avg rate)"
    type: number
    sql: sum(${warehouse_cost}) over (order by ${start_hour} rows between 6 preceding and current row) * 4 * 365 / 12;;
    value_format_name: usd
    drill_fields: [query_details*]
    required_fields: [start_hour]
  }

  measure: warehouse_cost_mtd {
    label: "Warehouse Cost (Month to Date)"
    required_fields: [start_date, start_month]
    type: number
    sql: sum(${warehouse_cost}) over (partition by ${start_month} order by ${start_date} rows unbounded preceding) ;;
    value_format_name: usd
  }

}
