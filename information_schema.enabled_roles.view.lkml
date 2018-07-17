view: enabled_roles {
  sql_table_name: INFORMATION_SCHEMA.ENABLED_ROLES ;;

  dimension: role_name {
    type: string
    sql: ${TABLE}.ROLE_NAME ;;
    primary_key: yes
  }

  dimension: role_owner {
    type: string
    sql: ${TABLE}.ROLE_OWNER ;;
  }

  measure: count {
    type: count
    drill_fields: [role_name]
  }
}
