connection: "snowflake_admin"

include: "information_schema.*"         # include all views in this project
include: "*.dashboard.lookml"  # include all dashboards in this project


explore: enabled_roles {
  join: applicable_roles {
    sql_on: ${enabled_roles.role_name} = ${applicable_roles.grantee} ;;
    relationship: one_to_many
    type: inner
  }

  join: object_privileges {
    sql_on: ${enabled_roles.role_name} = ${object_privileges.grantee};;
    relationship: one_to_many
    type: inner
  }
}
