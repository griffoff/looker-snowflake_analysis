view: t_role_grants {
  label: "Permissions"
  sql_table_name: ZPG.T_ROLE_GRANTS ;;

  set: details{
    fields: [object_database, object_schema, object_name, object_type, all_privileges, all_privileges_count, user_roles, role_paths, leaf_roles]
  }

  set: curated_fields{
    fields: [details*, object_database, object_schema, privilege, db_count, schema_count, object_count, -role_name]
  }

  measure: user_roles {
    type: string
    sql: ${t_user_roles.roles} ;;
  }

  dimension: pk {
    sql: array_to_string(
      array_construct(${root_path}, ${privilege}, ${object_name}, ${object_type})
      ,'.');;
    hidden: yes
    primary_key: yes
  }

  dimension: full_object_name {
    type: string
    sql: ${TABLE}.OBJECT_NAME ;;
  }

  dimension: db_na {
    hidden: yes
    sql:  ${object_type} in ('WAREHOUSE', 'USER', 'RESOURCE_MONITOR', 'EMAIL', 'NOTIFICATION_SUBSCRIPTION', 'ACCOUNT') ;;
  }

  dimension: object_database {
    type: string
    sql: case when ${db_na} then 'N/A' else split_part(${full_object_name}, '.', 1) end ;;
  }

  dimension: object_schema {
    type: string
    sql: case when ${db_na} then 'N/A' else split_part(${full_object_name}, '.', 2) end;;
  }

  dimension: object_name {
    type: string
    sql: split_part(${full_object_name}, '.', -1);;
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

  dimension: leaf_role {
    type: string
    sql: ${root_path}[array_size(${root_path})-1] ;;
    hidden: yes
  }

  measure: leaf_roles {
    label: "Leaf Roles"
    description: "Lowest level roles that give direct access to this resource"
    sql: listagg(distinct ${leaf_role}, ', ') ;;
  }

  measure: role_paths {
    label: "Role Paths"
    description: "full grant hierarchies that gives access to this resource"
    sql: listagg(distinct ${root_path}, '\n,') within group (order by ${root_path}) ;;
  }

  measure: count {
    type: count
    drill_fields: [details*]
  }

  measure: schema_count {
    type: count_distinct
    sql: ${object_schema} ;;
  }

  measure: db_count {
    type: count_distinct
    sql: ${object_database} ;;
  }

  measure: object_count {
    type: count_distinct
    sql: ${full_object_name} ;;
  }

  measure: all_privileges {
    type: string
       sql: array_to_string(array_agg(distinct ${privilege}) within group (order by ${privilege}), ', ') ;;
    drill_fields: [details*]
  }

  measure: all_privileges_count {
    label: "# privileges"
    type: number
    sql:  array_size(array_agg(distinct ${privilege}) within group (order by ${privilege}));;
    drill_fields: [details*]
  }

  measure: all_objects {
    type: string
    sql:  array_to_string(array_agg(distinct ${object_name}) within group (order by ${object_name}), ', ') ;;
  }
}
