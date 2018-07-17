view: object_privileges {
  sql_table_name: INFORMATION_SCHEMA.OBJECT_PRIVILEGES ;;

  set: details{
    fields: [grantee, object_name, privilege_type, created_date]
  }

  dimension: pk {
    sql: array_to_string(
      array_construct(${grantee}, ${object_catalog}, ${object_schema}, ${object_name}, ${privilege_type})
      ,'.');;
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
    type: yesno
    sql: ${TABLE}.IS_GRANTABLE='YES' ;;
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

  measure: all_privileges_arr {
    sql: array_agg(distinct ${privilege_type}) within group (order by ${privilege_type}) ;;
    drill_fields: [details*]
    hidden: yes
  }

  measure: all_privileges {
    sql: array_to_string(${all_privileges_arr}, ', ') ;;
    drill_fields: [details*]
  }

  measure: read {
    type: number
    sql: case
            when array_position('OWNERSHIP'::variant, ${all_privileges_arr}) >= 0 then 1
            when array_position('SELECT'::variant, ${all_privileges_arr}) >= 0 then 1
            end ;;
  }

  measure: write {
    type: number
    sql: case
            when array_position('OWNERSHIP'::variant, ${all_privileges_arr}) >= 0 then 1
            when array_position('UPDATE'::variant, ${all_privileges_arr}) >= 0 then 1
            when array_position('INSERT'::variant, ${all_privileges_arr}) >= 0 then 1
            when array_position('DELETE'::variant, ${all_privileges_arr}) >= 0 then 1
            when array_position('TRUNCATE'::variant, ${all_privileges_arr}) >= 0 then 1
            end;;
  }

  measure: ownership {
    type: number
    sql: case
            when array_position('OWNERSHIP'::variant, ${all_privileges_arr}) >= 0 then 1
            end;;
  }

  measure: grantable{
    type: sum
    sql: case when ${is_grantable} then 1 end;;
  }

  measure: count {
    type: count
    drill_fields: [details*]
  }
}
