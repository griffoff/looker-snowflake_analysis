view: t_user_roles {
  label: "User Roles"
  sql_table_name: ZPG.T_USER_ROLES ;;

  set: details{
    fields: [role_name, user_name]
  }

  dimension: pk {
    sql: array_to_string(
      array_construct(${role_name}, ${user_name})
      ,'.');;
    hidden: yes
    primary_key: yes
  }

  dimension: role_name {
    type: string
    sql: ${TABLE}.ROLE_NAME ;;
  }

  dimension: user_name {
    type: string
    sql: ${TABLE}.USER_NAME ;;
  }

  measure: count {
    type: count
    drill_fields: [user_name, role_name]
  }

  measure: roles {
    description: "A list of the roles this user can use"
    type: string
    sql:  array_to_string(array_agg(distinct ${role_name}) within group (order by ${role_name}), ', ') ;;
  }
}
