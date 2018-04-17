view: t_role_grants {
  label: "Permissions"
  sql_table_name: ZPG.T_ROLE_GRANTS ;;

  set: details{
    fields: [role_name, root_path, object_name, object_type, all_privileges]
  }

  set: curated_fields{
    fields: [details*, object_database, object_schema, privilege, -role_name]
  }

  dimension: pk {
    sql: array_to_string(
      array_construct(${root_path}, ${privilege}, ${object_name}, ${object_type})
      ,'.');;
    hidden: yes
    primary_key: yes
  }

  dimension: object_database {
    type: string
    sql: split_part(${object_name}, '.', 1) ;;
  }

  dimension: object_schema {
    type: string
    sql: split_part(${object_name}, '.', 2) ;;
  }

  dimension: object_name {
    type: string
    sql: ${TABLE}.OBJECT_NAME ;;
  }

  dimension: object_type {
    type: string
    sql: ${TABLE}.OBJECT_TYPE ;;
  }

  dimension: privilege {
    type: string
    sql: ${TABLE}.PRIVILEGE ;;
  }

  dimension: role_name {
    type: string
    sql: ${TABLE}.ROLE_NAME ;;
  }

  dimension: root_path {
    label: "Role Path"
    type: string
    sql: ${TABLE}.ROOT_PATH ;;
  }

  measure: count {
    type: count
    drill_fields: [details*]
  }

  measure: all_privileges {
    type: string
       sql: array_to_string(array_agg(distinct ${privilege}) within group (order by ${privilege}), ', ') ;;
    drill_fields: [details*]
  }

  measure: all_objects {
    type: string
    sql:  array_to_string(array_agg(distinct ${object_name}) within group (order by ${object_name}), ', ') ;;
  }
}
