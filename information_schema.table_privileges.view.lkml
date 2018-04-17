view: table_privileges {
  sql_table_name: INFORMATION_SCHEMA.TABLE_PRIVILEGES ;;

 dimension: pk {
    sql: array_construct(${grantee}, ${table_catalog}, ${table_schema}, ${table_name}, ${privilege_type}) ;;
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

  dimension: privilege_type {
    type: string
    sql: ${TABLE}.PRIVILEGE_TYPE ;;
  }

  dimension: table_catalog {
    type: string
    sql: ${TABLE}.TABLE_CATALOG ;;
  }

  dimension: table_name {
    type: string
    sql: ${TABLE}.TABLE_NAME ;;
  }

  dimension: table_schema {
    type: string
    sql: ${TABLE}.TABLE_SCHEMA ;;
  }

  dimension: with_hierarchy {
    type: string
    sql: ${TABLE}.WITH_HIERARCHY ;;
  }

  measure: count {
    type: count
    drill_fields: [table_name]
  }
}
