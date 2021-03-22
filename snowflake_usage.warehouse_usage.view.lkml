view: warehouse_usage {
  label: "Warehouse Usage"
  derived_table: {
    create_process: {
      sql_step:
        CREATE TRANSIENT TABLE IF NOT EXISTS looker_scratch.warehouse_usage_final
        CLUSTER BY (query_start_time::DATE)
        (
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
              ID NUMBER(18,0),
              QUERY_TAG_USER_NAME STRING
            )
        ;;
      sql_step:
        create sequence if not exists looker_scratch.warehouse_usage_id
        ;;
      sql_step:
        create transient table if not exists looker_scratch.warehouse_usage
        as
        select
          START_TIME, END_TIME, WAREHOUSE_ID, WAREHOUSE_NAME, CREDITS_USED
        from snowflake.account_usage.warehouse_metering_history
        order by start_time
        ;;
      sql_step:
        alter table looker_scratch.warehouse_usage cluster by (start_time::DATE)
        ;;
      sql_step:
        create or replace temporary table looker_scratch.wu_new
        as
        select
          START_TIME, END_TIME, WAREHOUSE_ID, WAREHOUSE_NAME, CREDITS_USED
        from snowflake.account_usage.warehouse_metering_history
        where start_time > (select max(start_time) from looker_scratch.warehouse_usage)
        ;;
      sql_step:
        insert into looker_scratch.warehouse_usage
        select * from looker_scratch.wu_new
        ;;
      sql_step:
        alter table looker_scratch.warehouse_usage recluster
        ;;
      sql_step:
        create transient table if not exists looker_scratch.warehouse_usage_detail
        as
        select
          QUERY_ID, QUERY_TEXT, QUERY_TYPE, SESSION_ID, USER_NAME, ROLE_NAME, SCHEMA_ID, SCHEMA_NAME, DATABASE_ID, DATABASE_NAME, WAREHOUSE_ID, WAREHOUSE_NAME, WAREHOUSE_TYPE
          ,WAREHOUSE_SIZE, QUERY_TAG, EXECUTION_STATUS, ERROR_CODE, ERROR_MESSAGE, START_TIME, END_TIME, TOTAL_ELAPSED_TIME, COMPILATION_TIME, EXECUTION_TIME, QUEUED_PROVISIONING_TIME
          ,QUEUED_REPAIR_TIME, QUEUED_OVERLOAD_TIME, TRANSACTION_BLOCKED_TIME, OUTBOUND_DATA_TRANSFER_REGION, OUTBOUND_DATA_TRANSFER_BYTES
        from snowflake.account_usage.query_history
        where execution_status != 'running'
        and NOT is_client_generated_statement

        order by start_time
        ;;
      sql_step:
        alter table looker_scratch.warehouse_usage_detail cluster by (start_time::DATE)
        ;;
      sql_step:
        create or replace temporary table looker_scratch.wud_new
        as
        select
          QUERY_ID, QUERY_TEXT, QUERY_TYPE, SESSION_ID, USER_NAME, ROLE_NAME, SCHEMA_ID, SCHEMA_NAME, DATABASE_ID, DATABASE_NAME, WAREHOUSE_ID, WAREHOUSE_NAME, WAREHOUSE_TYPE
          ,WAREHOUSE_SIZE, QUERY_TAG, EXECUTION_STATUS, ERROR_CODE, ERROR_MESSAGE, START_TIME, END_TIME, TOTAL_ELAPSED_TIME, COMPILATION_TIME, EXECUTION_TIME, QUEUED_PROVISIONING_TIME
          ,QUEUED_REPAIR_TIME, QUEUED_OVERLOAD_TIME, TRANSACTION_BLOCKED_TIME, OUTBOUND_DATA_TRANSFER_REGION, OUTBOUND_DATA_TRANSFER_BYTES
        from snowflake.account_usage.query_history
        where execution_status != 'running'
        and NOT is_client_generated_statement
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
        alter table looker_scratch.warehouse_usage_detail recluster
        ;;
      sql_step:
        delete from looker_scratch.warehouse_usage_final
        where start_time >= dateadd(day, -1, current_date())
        ;;
      sql_step:
        create or replace temporary table looker_scratch.users as
        select
            name as user_name, login_name as user_login_name, display_name as user_full_name, email as user_email
            ,case when user_email != '' then count(distinct user_name) over (partition by user_email) > 1 end as dup_emails
            ,row_number() over (partition by user_email order by case when user_login_name = user_email then 0 else 1 end) as preference
        from snowflake.account_usage.users
        where user_email != ''
        and user_login_name like '%@%'
        order by 4
        ;;
      sql_step:
        set latest = (select max(start_time) from looker_scratch.warehouse_usage_final)
        ;;
      sql_step:
        insert into looker_scratch.warehouse_usage_final
        select
          --coalesce(wud.start_time, wu.start_time) as start_time
          wu.start_time
          ,COALESCE(wud.start_time, wu.start_time) as query_start_time
          ,wu.warehouse_name, wu.credits_used as total_credits_used
          ,wud.query_id, wud.query_type, wud.query_text, wud.query_tag
          ,wud.user_name, wud.role_name, wud.database_name, wud.schema_name, wud.warehouse_size
          ,wud.execution_status, wud.error_message
          ,case
              when wud.end_time > wu.end_time --query runs over time boundary, this is the portion of the query BEFORE the boundary
              then date_part(epoch_millisecond, wu.end_time) - date_part(epoch_millisecond, wud.start_time) -- time boundary minus query start
              when wud.start_time < wu.start_time --query runs over time boundary, this is the portion of the query AFTER the boundary
              then date_part(epoch_millisecond, wud.end_time) - date_part(epoch_millisecond, wu.start_time) -- query end minus time boundary
              else total_elapsed_time
              end as elapsed_time_ms
          ,case
              when split_part(query_type, '_', 1) not in (
              'ALTER', 'CREATE', 'DESCRIBE', 'SHOW', 'RENAME', 'DROP', 'USE', 'SET', 'REVOKE', 'GRANT', 'UNSET'
              )
              then elapsed_time_ms
              when query_id is not null
              then 0
           end as elapsed_time_credit_use_ms
          ,sum(elapsed_time_credit_use_ms) over (partition by wu.start_time, wu.warehouse_name) as total_elapsed_time_credit_use_ms
          ,(elapsed_time_credit_use_ms / nullif(total_elapsed_time_credit_use_ms, 0))::decimal(20, 19) as credits_used_percent
          ,wu.credits_used * credits_used_percent as credits_used
          ,looker_scratch.warehouse_usage_id.nextval as id
          ,u.user_name as query_tag_user_name
        from looker_scratch.warehouse_usage wu
        left join looker_scratch.warehouse_usage_detail wud on wu.warehouse_name = wud.warehouse_name
                      and (
                          wud.start_time between wu.start_time and wu.end_time
                          or
                          wud.end_time between wu.start_time and wu.end_time --query may be in more than one hour bucket
                        )
        left join looker_scratch.users u on UPPER(wud.query_tag) in (UPPER(u.user_login_name), UPPER(u.user_email)) and u.preference = 1
        where (wu.start_time > $latest
                or $latest is null)
      ;;
      sql_step:
        alter table looker_scratch.warehouse_usage_final recluster ;;

      sql_step:
        create or replace transient table ${SQL_TABLE_NAME} clone looker_scratch.warehouse_usage_final
      ;;
    }
    sql_trigger_value: select max(start_time) from snowflake.account_usage.warehouse_metering_history  ;;
  }

  drill_fields: [
    start_time, query_text, query_type
    , warehouse_name, database_name, schema_name, table_name
    , role_name, user_name, query_tag
    , total_elapsed_time_detail, warehouse_cost
    , query_id
    ]

  dimension: database_name_raw {
    hidden: yes
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
    html: <a title="{{query_text._value}}" target="_blank" href="https://cengage.snowflakecomputing.com/console#/monitoring/queries/detail?queryId={{value}}">{{rendered_value}}</a> ;;
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

  dimension: query_type_category {
    group_label: "Query details"
    type: string
    case: {
      when: {
        label: "SELECT"
        sql:  ${query_type}= 'SELECT';;
        }
      when: {
        label: "DML"
        sql: ${query_type}= 'CREATE_TABLE_AS_SELECT'
            OR split_part(${query_type}, '_', 1) in (
              'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'ROLLBACK', 'COMMIT', 'BEGIN', 'RECLUSTER'
              );;
      }
      when: {
        label: "DDL"
        sql: split_part(${query_type}, '_', 1) in (
              'ALTER', 'CREATE', 'DESCRIBE', 'SHOW', 'RENAME', 'DROP', 'USE', 'SET', 'REVOKE', 'GRANT', 'UNSET'
              );;
      }
        else: "UNKNOWN"
    }
  }

  dimension: role_name {
    type: string
    sql: ${TABLE}.ROLE_NAME ;;
  }

  dimension: schema_name_raw {
    hidden: yes
    type: string
    sql: ${TABLE}.SCHEMA_NAME ;;
  }

  dimension: query_text_clean {
    group_label: "Query details"
    sql: UPPER(replace(regexp_replace(${query_text}, '\\s+', ' '), '""', '')) ;;
  }

   dimension: table_name_calc_base {
    hidden: yes
    group_label: "Query details"
    sql:
        TRIM(UPPER(CASE
        WHEN query_type = 'MERGE' THEN ARRAY_COMPACT(split(${query_text_clean}, ' '))[2]
        WHEN query_type IN ('SELECT', 'UNLOAD') THEN
          split_part(ARRAY_COMPACT(split(
            CASE WHEN LEFT(TRIM(split_part(${query_text_clean}, 'FROM ', 2)), 1) = '('
            THEN
              REGEXP_SUBSTR(${query_text_clean}, '.*FROM\\s+\\((.+)\\).*', 1, 1, 'me') --to extract subqueries e.g. SELECT * FROM (SELECT * FROM table)
            ELSE
              ${query_text_clean}
            END
            , 'FROM '))[1], ' ', 1)
        WHEN query_type = 'CREATE_TABLE_AS_SELECT' THEN split_part(split_part(ARRAY_COMPACT(split(${query_text_clean}, 'TABLE '))[1], ' AS ', 1), 'COPY GRANTS', 1)
        WHEN query_type IN ('INSERT', 'DELETE', 'RECLUSTER', 'COPY', 'UNLOAD', 'DESCRIBE_QUERY')
          THEN ARRAY_COMPACT(split(${query_text_clean}, ' '))[2]
        WHEN query_type = 'UPDATE' THEN ARRAY_COMPACT(split(${query_text_clean}, ' '))[1]
        ELSE '?'
        END))
 ;;
  }

  dimension: table_name {
    group_label: "Query details"
    sql: COALESCE(SPLIT_PART(${table_name_calc_base}, '.', -1), ${tablename})::STRING;;
    alias: [table_name_calc]
  }

  dimension: full_table_name {
    group_label: "Query details"
    sql: ${table_name_calc_base}::STRING ;;
  }

  dimension: schema_name {
    group_label: "Query details"
    sql: COALESCE(
            NULLIF(SPLIT_PART(${table_name_calc_base}, '.', -2), '')
             ,${schema_name_raw}
             )::STRING ;;
    alias: [schema_name_calc]
  }

  dimension: full_schema_name {
    group_label: "Query details"
    sql: ${database_name} || '.' || ${schema_name} ;;
    link: {
      label: "See more detailed usage for this schema"
      url: "https://cengage.looker.com/dashboards-next/889?Full+Schema+Name={{value | url_encode}}"
    }
  }

  dimension: database_name {
    group_label: "Query details"
    sql: COALESCE(
            NULLIF(SPLIT_PART(${table_name_calc_base}, '.', -3), '')
             ,${database_name_raw}
             )::STRING ;;
    alias: [database_name_calc]
  }

  dimension: usage_date {
    type: date_raw
    sql: ${start_raw}::date ;;
    hidden: yes
  }

  measure: example_query_text {
    description: "An example query in the current context of rows or pivot dimensions.  Limited to 500 characters"
    type: string
    sql: ANY_VALUE(${query_text_clean}) ;;
    html: <div title="{{value}}">{{value | slice:0,500}}</div>  ;;
  }

  dimension_group: start {
    label: "Query Start"
    type: time
    sql: ${TABLE}.QUERY_START_TIME ;;
  }

  dimension_group: end {
    label: "Query End"
    type: time
    sql: DATEADD(millisecond, ${elapsed_time_ms}, ${TABLE}.QUERY_START_TIME) ;;
  }

  dimension: start_week_of_month {
    group_label: "Query Start Date"
    label: "Week of Month"
    sql: 1 + FLOOR((${start_day_of_month}-1) / 7) ;;
  }

  measure: latest_start_time {
    label: "Up to date as of:"
    type: date_time
    sql: max(${start_raw}) ;;
  }

  measure: data_recency {
    label: "Last queried"
    type: date
    sql: ${latest_start_time};;
  }

  dimension: query_tag {
    group_label: "Query details"
  }
  dimension: query_tag_user_name {
    group_label: "User name"
    label: "Real user name"
    description: "Either from a session tag if available (looker users) or direct user in snowflake"
    sql: UPPER(CASE
            WHEN ${TABLE}.query_tag_user_name IS NOT NULL
            THEN ${TABLE}.query_tag_user_name
            WHEN ${query_tag} = 'PDT'
            THEN 'PDT Rebuild'
            WHEN ${query_tag} != ''
            THEN ${query_tag}
            ELSE ${user_name}
            END);;
    link: {
      label: "See more detailed usage for this user"
      url: "https://cengage.looker.com/dashboards-next/889?Real+user+name={{value | url_encode}}"}
  }

  dimension: user_name {
    group_label: "User name"
    label: "Database user name"
    type: string
    sql: ${TABLE}.USER_NAME ;;
    drill_fields: [query_tag, start_week, warehouse_cost, count]
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
        sql: ${user_name} ilike 'FIVETRAN%' or ${warehouse_name} ilike 'dbsync' ;;
        label: "Data Sync - FiveTran"
      }
      when: {
        sql: ${user_name} ilike 'REALTIME_SYNC%' ;;
        label: "Data Sync - RealTime"
      }
      when: {
        sql:  ${query_tag_user_name} ilike 'PDT Rebuild' ;;
        label: "Service - Looker - PDT Rebuild"
      }
      when: {
        sql:  ${user_name} ilike 'LOOKER%' ;;
        label: "Service - Looker - Reporting"
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
      when: {
        sql:  ${user_name} ilike 'APP_%' ;;
        label: "Service - App/Other"
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
  }

  dimension: elapsed_time_ms {
    type: number
    hidden: no
  }

  dimension: elapsed_time_s {
    type: number
    sql: ${elapsed_time_ms} / 1000 ;;
    hidden: no
  }

  dimension: elapsed_time {
    type: number
    sql: ${elapsed_time_s} / 3600 / 24 ;;
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

  dimension: tablename {
    hidden: yes
    sql: replace(split_part(${query_text}, 'AS', 1), 'CREATE TABLE', '')  ;;
  }

  dimension: CTAS_tablename {
    group_label: "Query details"
    description: "The name of the table if it is a CREATE TABLE AS SELECT statement"
    #CREATE TABLE LOOKER_SCRATCH.LC$JJ3T41537466855111_fair_use_tracking AS
    sql:
      case
        when ${query_type} = 'CREATE_TABLE_AS_SELECT'
        then
          case
            when ${query_text} ilike '%LOOKER_SCRATCH.LC$%'
            then
              'LOOKER_SCRATCH.' || array_to_string(array_slice(split(${tablename}, '_'), 2, 99), '_')
            else ${tablename}
            end
        end
    ;;
  }

  measure: total_elapsed_time {
    label: "Query Time (Total)"
    group_label: "Query Duration"
    type: sum
    sql: ${elapsed_time} ;;
    value_format: "[m] \m\i\n\s s \s\e\c\s"
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
  }

  measure: max_elapsed_time {
    label: "Query Time (Max)"
    group_label: "Query Duration"
    type: max
    sql: ${elapsed_time} ;;
    value_format_name: duration_hms

  }

  measure: min_elapsed_time {
    label: "Query Time (Min)"
    group_label: "Query Duration"
    type: min
    sql: ${elapsed_time} ;;
    value_format_name: duration_hms

  }

  measure: med_elapsed_time {
    label: "Query Time (Median)"
    group_label: "Query Duration"
    type: median
    sql: ${elapsed_time} ;;
    value_format_name: duration_hms

  }

  measure: stdev_elapsed_time {
    label: "Query Time (Std. Dev.)"
    group_label: "Query Duration"
    type: number
    sql: stddev(${elapsed_time}) ;;
    value_format_name: duration_hms

  }

  measure: total_elapsed_time_credit_use {
    label: "Total Chargable Query Time"
    type: sum
    sql: ${TABLE}.total_elapsed_time_credit_use_ms / 3600 / 24 ;;
    value_format_name: duration_hms

  }

  measure: credits_used_percent {
    type: sum
    value_format_name: percent_2
    hidden: yes
  }

  dimension: credits_used {
    type: number
    value_format_name: decimal_2

    hidden: yes
  }

  dimension: warehouse_cost_per_credit {
    type: number
    sql: CASE WHEN ${usage_date} >= '2018-11-18' THEN 1.25 ELSE 1.4 END ;;
    hidden: yes
  }

  dimension: warehouse_cost_raw {
    label:"Warehouse Cost"
    type: number
    sql: case when ${credits_used} >= 0 then ${credits_used} end * ${warehouse_cost_per_credit};;
    value_format_name: usd
    hidden: yes

  }

  measure: warehouse_cost {
    group_label: "Warehouse Cost"
    label:"Warehouse Cost"
    type: sum
    sql: ${warehouse_cost_raw} ;;
    value_format_name: usd

  }

  measure: warehouse_cost_pit {
    type: sum
    group_label: "Warehouse Cost"
    label: "Warehouse Cost (Point in Time)"
    description: "The total cost of running warehouses (processing units) up the same point in the month as yesterday
    i.e. running a query on 21st June for the past two months will show you June's current cost and May's cost up until May 20th"
    sql: IFF(${warehouse_usage.start_day_of_month} <= DATE_PART(day, DATEADD(day, -1, CURRENT_DATE())), ${warehouse_cost_raw}, NULL) ;;
    value_format_name: usd

  }

  measure: warehouse_cost_avg {
    group_label: "Warehouse Cost"
    label:"Warehouse Cost (Avg)"
    type: number
    sql: ${warehouse_cost} / ${count};;
    value_format_name: usd

  }

  measure: warehouse_cost_monthly {
    group_label: "Warehouse Cost"
    label:"Warehouse Cost (1 month at this rate)"
    type: number
    sql: ${warehouse_cost_raw} * 365 / 12;;
    value_format_name: usd

    required_fields: [start_date]
  }

  measure: warehouse_cost_ytd {
    group_label: "Warehouse Cost"
    label:"Warehouse Cost (YTD)"
    type: number
    sql: SUM(${warehouse_cost_raw}) OVER (PARTITION BY YEAR(${usage_date} ORDER BY ${start_date} ROWS UNBOUNDED preceding) ;;
    value_format_name: usd
    required_fields: [start_date]
  }


  measure: warehouse_cost_monthly_day_2 {
    group_label: "Warehouse Cost"
    label:"Warehouse Cost (1 month at the last 7 days avg daily rate)"
    type: number
    sql: sum(${warehouse_cost_raw}) over (order by ${start_date} rows between 7 preceding and current row) * 365 / 12 / 7 ;;
    value_format_name: usd
    required_fields: [start_date]
  }

  measure: warehouse_cost_monthly_6 {
    group_label: "Warehouse Cost"
    label:"Warehouse Cost (1 month at the last 6 hours avg rate)"
    type: number
    sql: sum(${warehouse_cost_raw}) over (order by ${start_hour} rows between 6 preceding and current row) * 4 * 365 / 12;;
    value_format_name: usd

    required_fields: [start_hour]
  }

  measure: warehouse_cost_mtd {
    label: "Warehouse Cost (Month to Date)"
    required_fields: [start_date, start_month]
    type: number
    sql: sum(${warehouse_cost_raw}) over (partition by ${start_month} order by ${start_date} rows unbounded preceding) ;;
    value_format_name: usd
  }

}
