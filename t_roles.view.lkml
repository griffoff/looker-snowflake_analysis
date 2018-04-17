view: t_roles {
  label: "Roles"
  sql_table_name: ZPG.T_ROLES ;;

  dimension: role_comment {
    type: string
    sql: ${TABLE}.ROLE_COMMENT ;;
  }

  dimension: role_name {
    type: string
    sql: ${TABLE}.ROLE_NAME ;;
    primary_key: yes
  }

  measure: count {
    type: count
    drill_fields: [role_name, role_comment]
  }

  measure: roles_available {
    type: string
    sql: array_to_string(array_agg(distinct ${role_name}) within group (order by ${role_name}), ',') ;;
  }
}
