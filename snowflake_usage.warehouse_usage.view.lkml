view: warehouse_usage {
  label: "Warehouse Usage"
  #sql_table_name: ZPG.WAREHOUSE_USAGE_DETAIL ;;
  derived_table: {
# TEMPORARY FIX FOR WHILE THE USAGE AND STORAGE TABLES ARE NOT BEING POPULATED
sql: with wh_detail as (
            select
              query_id
              ,start_time
              ,start_hour
              ,warehouse_name
              ,database_name
              ,schema_name
              ,user_name
              ,role_name
              ,warehouse_size
              ,query_text
              ,total_elapsed_time as total_elapsed_time_ms
              ,case when query_type not in ('DROP_CONSTRAINT', 'ALTER_TABLE_MODIFY_COLUMN', 'ALTER_TABLE_ADD_COLUMN', 'ALTER_TABLE_DROP_COLUMN', 'RENAME_COLUMN', 'DESCRIBE', 'SHOW', 'CREATE_TABLE', 'ALTER_SESSION', 'USE', 'DROP', 'CREATE CONSTRAINT', 'ALTER USER')
                    then total_elapsed_time_ms
                    when query_id is not null
                    then 0
                    end as total_elapsed_time_credit_use_ms
              ,case
                when query_type = 'UNKNOWN'
                  then array_to_string(array_slice(split(query_text, ' '), 0, 2), ' ')
                else query_type
                end as query_type
            from USAGE.SNOWFLAKE.WAREHOUSE_USAGE_DETAIL
        )
        ,wh_usage as (
            select warehouse_name, start_time, credits_used
            from USAGE.SNOWFLAKE.WAREHOUSE_USAGE wu
            union
            select warehouse_name, start_hour, (sum(total_elapsed_time_credit_use_ms) / 1000 / 3600) * 1.4
            from wh_detail
            group by 1, 2
        )
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
            ,wud.total_elapsed_time_ms
            ,wud.total_elapsed_time_credit_use_ms
            ,wud.query_text
            ,total_elapsed_time_credit_use_ms / nullif(sum(total_elapsed_time_credit_use_ms) over (partition by wu.start_time, wu.warehouse_name), 0) as credits_used_percent
            ,coalesce(credits_used_percent, 1) * total_credits_used as credits_used
            ,row_number() over (order by (wud.start_time, wu.start_time), wu.warehouse_name) as id
        from wh_usage wu
        left join wh_detail wud on wu.warehouse_name = wud.warehouse_name
                                                --and wu.start_time = wud.start_hour
                                                --accommodate queries that run across an hour boundary (this causes nulls to show up when there are no other queries in the following hour)
                                                and wu.start_time >= wud.start_hour
                                                and wu.start_time <= date_trunc(hour, dateadd(millisecond, wud.total_elapsed_time_ms, wud.start_time))
        where wu.start_time >= '2016-11-01' ;;
#    sql:
#       select
#         wu.warehouse_name
#         ,coalesce(wud.start_time, wu.start_time) as start_time
#         ,wu.credits_used as total_credits_used
#         ,wud.query_id
#         ,wud.database_name
#         ,wud.schema_name
#         ,case
#             when wud.query_type = 'UNKNOWN'
#               then array_to_string(array_slice(split(query_text, ' '), 0, 2), ' ')
#             else wud.query_type
#             end as query_type
#         ,wud.user_name
#         ,wud.role_name
#         ,wud.warehouse_size
#         ,wud.total_elapsed_time as total_elapsed_time_ms
#         ,case when query_type not in ('DROP_CONSTRAINT', 'ALTER_TABLE_MODIFY_COLUMN', 'ALTER_TABLE_ADD_COLUMN', 'ALTER_TABLE_DROP_COLUMN', 'RENAME_COLUMN', 'DESCRIBE', 'SHOW', 'CREATE_TABLE', 'ALTER_SESSION', 'USE', 'DROP', 'CREATE CONSTRAINT', 'ALTER USER')
#               then total_elapsed_time_ms
#               when query_id is not null
#               then 0
#               end as total_elapsed_time_credit_use_ms
#         ,wud.query_text
#         ,total_elapsed_time_credit_use_ms / nullif(sum(total_elapsed_time_credit_use_ms) over (partition by wu.start_time, wu.warehouse_name), 0) as credits_used_percent
#         ,coalesce(credits_used_percent, 1) * total_credits_used as credits_used
#         ,row_number() over (order by (wud.start_time, wu.start_time), wu.warehouse_name) as id
#     from  USAGE.SNOWFLAKE.WAREHOUSE_USAGE wu
#     left join USAGE.SNOWFLAKE.WAREHOUSE_USAGE_DETAIL wud on wu.warehouse_name = wud.warehouse_name
#                                             --and wu.start_time = wud.start_hour
#                                             --accommodate queries that run across an hour boundary (this causes nulls to show up when there are no other queries in the following hour)
#                                             and wu.start_time >= wud.start_hour
#                                             and wu.start_time <= date_trunc(hour, dateadd(millisecond, wud.total_elapsed_time, wud.start_time))
#     where wu.start_time >= '2017-11-01'
#
#    ;;

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
    sql: ${TABLE}.start_time::date ;;
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
    sql: ${TABLE}.START_TIME ;;
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
    value_format: "h \h\r\s m \m\i\n\s"
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
    value_format_name: currency
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
    sql: ${credits_used} * ${warehouse_cost_per_credit};;
    value_format_name: currency
    drill_fields: [query_details*]
  }

  measure: warehouse_cost_avg {
    label:"Warehouse Cost (Avg)"
    type: number
    sql: ${warehouse_cost} / ${count};;
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
    label: "Warehouse Cost (Month to Date)"
    required_fields: [start_date, start_month]
    type: number
    sql: sum(${warehouse_cost}) over (partition by ${start_month} order by ${start_date} rows unbounded preceding) ;;
    value_format_name: currency
  }

}
