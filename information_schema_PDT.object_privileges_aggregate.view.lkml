include: "snowflake_permissions.model.lkml"
explore: object_privileges_aggregate {}

view: object_privileges_aggregate {
    derived_table: {
      explore_source: enabled_roles {
        column: grantor { field: object_privileges.grantor }
        column: grantee { field: applicable_roles.grantee }
        column: object_catalog { field: object_privileges.object_catalog }
        column: object_schema { field: object_privileges.object_schema }
        column: object_name { field: object_privileges.object_name }
        column: object_type { field: object_privileges.object_type }
        column: created_date { field: object_privileges.created_date }
        column: is_grantable { field: object_privileges.is_grantable }
        column: all_privileges { field: object_privileges.all_privileges }
        column: all_privileges_arr { field: object_privileges.all_privileges_arr }
        column: read {field:object_privileges.read}
        column: write {field:object_privileges.write}
        column: grantable {field:object_privileges.grantable}
        column: ownership {field:object_privileges.ownership}
        filters: {
          field: enabled_roles.role_name
          value: "-ACCOUNTADMIN"
        }
      }
    }

    set: details {
      fields: [grantor, grantee, object_type, full_object_name, created_date, is_grantable, all_privileges]
    }
    dimension: grantor {}
    dimension: grantee {}
    dimension: object_catalog {}
    dimension: object_schema {}
    dimension: object_name {hidden:yes}
    dimension: object_type {}
    dimension: full_object_name{
      label: "Object Name"
      sql: coalesce(${object_catalog}||'.', '') || coalesce(${object_schema}||'.', '')||${object_name} ;;
    }
    dimension: is_grantable {type:yesno sql:${TABLE}.is_grantable='YES';;}

    dimension: created_date {
      type: date_raw
    }
    dimension: all_privileges {
      type: string
    }
    dimension: all_privileges_arr {
      hidden: yes
    }

    measure: read {
      type: number
      drill_fields: [details*]
      sql: nullif(sum(${TABLE}.read), 0) ;;
    }

    measure: write {
      type: number
      drill_fields: [details*]
      sql: nullif(sum(${TABLE}.write), 0) ;;
    }

    measure: grantable{
      type: number
      drill_fields: [details*]
      sql: nullif(sum(${TABLE}.grantable), 0) ;;
    }

    measure: ownership{
      type: number
      drill_fields: [details*]
      sql: nullif(sum(${TABLE}.ownership), 0) ;;
    }



  }
