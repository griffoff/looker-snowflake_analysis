view: applicable_roles {
  sql_table_name: INFORMATION_SCHEMA.APPLICABLE_ROLES ;;

  dimension: pk {
    sql: array_to_string(array_construct(${role_name}, ${grantee}),'.') ;;
    hidden: yes
    primary_key: yes
  }

  dimension: grantee {
    type: string
    sql: ${TABLE}.GRANTEE ;;
  }

  dimension: is_grantable {
    type: string
    sql: ${TABLE}.IS_GRANTABLE ;;
  }

  dimension: role_name {
    type: string
    sql: ${TABLE}.ROLE_NAME ;;
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
