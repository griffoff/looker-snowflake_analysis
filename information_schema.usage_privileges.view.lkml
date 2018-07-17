view: usage_privileges {
  sql_table_name: INFORMATION_SCHEMA.USAGE_PRIVILEGES ;;

  dimension: pk {
    sql: array_construct(${grantee}, ${object_catalog}, ${object_schema}, ${object_name}, ${privilege_type}) ;;
    hidden: yes
    primary_key: yes
  }

  dimension_group: created {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.CREATED ;;
  }

  dimension: grantee {
    type: string
    sql: ${TABLE}.GRANTEE ;;
  }

  dimension: grantor {
    type: string
    sql: ${TABLE}.GRANTOR ;;
  }

  dimension: is_grantable {
    type: string
    sql: ${TABLE}.IS_GRANTABLE ;;
  }

  dimension: object_catalog {
    type: string
    sql: ${TABLE}.OBJECT_CATALOG ;;
  }

  dimension: object_name {
    type: string
    sql: ${TABLE}.OBJECT_NAME ;;
  }

  dimension: object_schema {
    type: string
    sql: ${TABLE}.OBJECT_SCHEMA ;;
  }

  dimension: object_type {
    type: string
    sql: ${TABLE}.OBJECT_TYPE ;;
  }

  dimension: privilege_type {
    type: string
    sql: ${TABLE}.PRIVILEGE_TYPE ;;
  }

  measure: count {
    type: count
    drill_fields: [object_name]
  }
}
