include: "//core/datagroups.lkml"

view: t_privilege_type {
  label: "Permissions"
  derived_table: {
    sql:
      select
        column1 as privilege
        ,column2 as privilege_type
    from values
    ('CREATE DATABASE','CREATE')
    ,('CREATE FILE FORMAT','CREATE')
    ,('CREATE FUNCTION','CREATE')
    ,('CREATE PIPE','CREATE')
    ,('CREATE ROLE','CREATE')
    ,('CREATE SCHEMA','CREATE')
    ,('CREATE SEQUENCE','CREATE')
    ,('CREATE STAGE','CREATE')
    ,('CREATE TABLE','CREATE')
    ,('CREATE USER','CREATE')
    ,('CREATE VIEW','CREATE')
    ,('CREATE WAREHOUSE','CREATE')
    ,('DELETE','DML')
    ,('INSERT','DML')
    ,('MANAGE GRANTS','ADMIN')
    ,('MODIFY','DML')
    ,('MONITOR','MONITOR')
    ,('MONITOR USAGE','MONITOR')
    ,('OPERATE','READ')
    ,('OWNERSHIP','ADMIN')
    ,('READ','READ')
    ,('REFERENCES','READ')
    ,('SELECT','READ')
    ,('TRUNCATE','DML')
    ,('UPDATE','DML')
    ,('USAGE','READ');;

    datagroup_trigger: daily_refresh
  }

  dimension: privilege {
    primary_key: yes
    hidden: yes
  }

  dimension: privilege_type {}
}
