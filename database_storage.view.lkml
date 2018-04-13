view: database_storage {
  #sql_table_name: ZPG.DATABASE_STORAGE ;;
  derived_table: {
    sql:
      with daily as (
        select
            *
            ,(average_database_bytes + average_failsafe_bytes) / power(1024, 4) as db_tb
            ,sum(db_tb) over (partition by usage_date) as total_tb
        FROM ZPG.DATABASE_STORAGE
        )
        select
            *
            ,avg(total_tb) over (partition by date_trunc(month, usage_date)) as monthly_total_tb
            ,avg(db_tb) over (partition by database_name, date_trunc(month, usage_date)) as monthly_db_tb
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

  measure: periods {
    type: number
    sql:
      {% if database_storage.usage_hour._in_query %}
        max(EXTRACT(day FROM LAST_DAY(${usage_hour}::date)))) * 24
      {% elsif warehouse_usage.start_hour._in_query %}
        max(EXTRACT(day FROM LAST_DAY(${warehouse_usage.start_hour}::date))) * 24
      {% elsif database_storage.usage_date._in_query %}
        max(EXTRACT(day FROM LAST_DAY(${usage_date}::date)))
      {% elsif warehouse_usage.start_date._in_query %}
        max(EXTRACT(day FROM LAST_DAY(${warehouse_usage.start_date}::date)))
      {% elsif warehouse_usage.start_week._in_query and warehouse_usage.start_day_of_week._in_query %}
        24
      {% elsif warehouse_usage.start_week._in_query %}
        4
      {% elsif warehouse_usage.start_day_of_week._in_query %}
        7
        --count(distinct ${warehouse_usage.start_date})
      {% else %}
        1
      {% endif %}
      ;;
    hidden: yes
  }

  measure: monthly_db_tb {
    type: sum_distinct
    sql: ${TABLE}.monthly_db_tb ;;
    hidden: yes
    sql_distinct_key: ${usage_month} ;;
  }

  measure: db_tb {
    type: number
    sql:
      {% if database_storage.usage_hour._in_query %}
        avg(${TABLE}.db_tb)
      {% elsif warehouse_usage.start_hour._in_query %}
        avg(${TABLE}.db_tb)
      {% elsif database_storage.usage_date._in_query %}
        avg(${TABLE}.db_tb)
      {% elsif warehouse_usage.start_date._in_query %}
        avg(${TABLE}.db_tb)
      {% elsif database_storage.usage_week._in_query %}
        avg(${TABLE}.db_tb)
      {% elsif warehouse_usage.start_week._in_query %}
        avg(${TABLE}.db_tb)
      {% else %}
        ${monthly_db_tb}
      {% endif %}  ;;
      hidden: yes
  }

  measure: monthly_total_tb {
    type: sum_distinct
    sql: ${TABLE}.monthly_total_tb ;;
    hidden: yes
    sql_distinct_key: ${usage_month} ;;
  }

  measure: total_tb {
    type: number
    sql:
      {% if database_storage.usage_hour._in_query %}
        avg(${TABLE}.total_tb)
      {% elsif warehouse_usage.start_hour._in_query %}
        avg(${TABLE}.total_tb)
      {% elsif database_storage.usage_date._in_query %}
        avg(${TABLE}.total_tb)
      {% elsif warehouse_usage.start_date._in_query %}
        avg(${TABLE}.total_tb)
      {% elsif database_storage.usage_week._in_query %}
        avg(${TABLE}.total_tb)
      {% elsif warehouse_usage.start_week._in_query %}
        avg(${TABLE}.total_tb)
      {% else %}
        ${monthly_total_tb}
      {% endif %} ;;
      value_format_name: TB_1
      hidden: yes
  }

  measure: credit_usage_value {
    type: number
    sql:
      {% if database_storage.database_name._in_query %}
        ${db_tb}
      {% else %}
        ${total_tb}
      {% endif %}
      ;;
    value_format_name: TB_1
    hidden: yes
  }

  measure: credit_usage {
    type: number
    sql: ${credit_usage_value} / ${periods} ;;
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

  measure: storage_cost {
    type: number
    sql:  ${credit_usage} * ${storage_rate} ;;
    value_format_name: currency
  }

  measure: monthly_storage_cost {
    type: number
    sql:  ${monthly_total_tb} * ${storage_rate} ;;
    value_format_name: currency
    hidden: no
  }

  dimension: database_name {
    type: string
    sql: ${TABLE}.DATABASE_NAME ;;
  }


}
