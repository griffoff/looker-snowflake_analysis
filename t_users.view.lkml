view: t_users {
  label: "Users"
  sql_table_name: ZPG.T_USERS ;;

  dimension: default_role {
    type: string
    sql: ${TABLE}.DEFAULT_ROLE ;;
  }

  dimension: default_wh {
    type: string
    sql: ${TABLE}.DEFAULT_WH ;;
  }

  dimension: deleted {
    type: yesno
    sql: ${TABLE}.DELETED ;;
  }

  dimension: user_comment {
    type: string
    sql: ${TABLE}.USER_COMMENT ;;
  }

  dimension: user_email {
    type: string
    sql: ${TABLE}.USER_EMAIL ;;
  }

  dimension: user_first_name {
    type: string
    sql: ${TABLE}.USER_FIRST_NAME ;;
  }

  dimension: user_full_name {
    type: string
    sql: ${TABLE}.USER_FULL_NAME ;;
  }

  dimension: user_last_name {
    type: string
    sql: ${TABLE}.USER_LAST_NAME ;;
  }

  dimension: user_login_name {
    type: string
    sql: ${TABLE}.USER_LOGIN_NAME ;;
  }

  dimension: user_name {
    type: string
    sql: ${TABLE}.USER_NAME ;;
    primary_key: yes
  }

  measure: count {
    label: "# Users"
    type: count
    drill_fields: [user_name, user_login_name, user_full_name, user_first_name, user_last_name]
  }
}
