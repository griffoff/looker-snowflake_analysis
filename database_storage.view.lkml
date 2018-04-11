view: database_storage {
  #sql_table_name: ZPG.DATABASE_STORAGE ;;
  derived_table: {
    sql:
      select
          *
          ,(average_database_bytes + average_failsafe_bytes) / power(1024, 4) as db_tb
          ,sum(db_tb) over (partition by usage_date) as total_tb
      FROM ZPG.DATABASE_STORAGE  AS database_storage
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

  dimension: days_in_month {
    type: number
    sql:  EXTRACT(day FROM LAST_DAY(${usage_raw}));;
    hidden: yes
  }

  dimension: average_failsafe_bytes {
    type: number
    sql: ${TABLE}.AVERAGE_FAILSAFE_BYTES ;;
    hidden: yes
  }

  measure: credit_usage {
    type: number
    sql:
      {% if database_storage.database_name._in_query %}
        avg(${TABLE}.db_tb)
      {% else %}
        avg(${TABLE}.total_tb)
      {% endif %}
      ;;
    value_format_name: TB_1
  }

  dimension: storage_rate {
    type: number
    sql: 23;;
    #/ ${days_in_month};;
    hidden: yes
  }

  measure: storage_cost {
    type: number
    sql:  ${credit_usage} * ${storage_rate} ;;
    value_format_name: currency
  }

  dimension: database_name {
    type: string
    sql: ${TABLE}.DATABASE_NAME ;;
  }

  dimension_group: usage {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.USAGE_DATE ;;
  }

}
