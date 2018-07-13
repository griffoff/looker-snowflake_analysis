connection: "snowflake_dev"

include: "t_*.*"         # include all views in this project
#include: "*.dashboard.lookml"  # include all dashboards in this project

explore: database_permissions  {
  from:  t_roles
  view_name:  t_roles
  label: "Object Permissions"
  join: t_role_grants {
    fields: [t_role_grants.curated_fields*]
    sql_on: ${t_roles.role_name} = ${t_role_grants.role_name} ;;
    relationship: one_to_many
  }
  join: t_privilege_type {
    sql_on: ${t_role_grants.privilege} = ${t_privilege_type.privilege}  ;;
    relationship: many_to_one
  }
  join: t_user_roles {
    fields: []
    sql_on: ${t_roles.role_name} = ${t_user_roles.role_name};;
    relationship: one_to_many
  }
  join: t_users {
    sql_on: ${t_user_roles.user_name} = ${t_users.user_name} ;;
    relationship: many_to_one
  }
}

explore: user_roles {
  from: t_users
  view_name: t_users

  label: "User Roles"

  join: t_user_roles {
    sql_on: ${t_users.user_name} = ${t_user_roles.user_name};;
    relationship: one_to_many
  }

}
