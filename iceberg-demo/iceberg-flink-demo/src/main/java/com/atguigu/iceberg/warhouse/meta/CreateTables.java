package com.atguigu.iceberg.warhouse.meta;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *   构建Icerberg表
 */

public class CreateTables {

    public static void main(String[] args) {

        //获取表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //tableEnv.executeSql("SET execution.checkpointing.interval = 3s");
        tableEnv.executeSql("CREATE CATALOG hadoop_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                "  'warehouse'='file:///tmp/warehouse/iceberg',\n" +
                "  'property-version'='1'\n" +
                ")");

        tableEnv.executeSql("USE CATALOG hadoop_catalog");

        //dwd_member
        tableEnv.executeSql("create table iceberg.dwd_member(\n" +
                "   uid int,\n" +
                "   ad_id int,\n" +
                "   birthday string,\n" +
                "   email string,\n" +
                "   fullname string,\n" +
                "   iconurl string,\n" +
                "   lastlogin string,\n" +
                "   mailaddr string,\n" +
                "   memberlevel string,\n" +
                "   password string,\n" +
                "   paymoney string,\n" +
                "   phone string,\n" +
                "   qq string,\n" +
                "   register string,\n" +
                "   regupdatetime string,\n" +
                "   unitname string,\n" +
                "   userip string,\n" +
                "   zipcode string,\n" +
                "   dt string)\n" +
                "   partitioned by(dt)");

        //dwd_member_regtype
        tableEnv.executeSql("create table iceberg.dwd_member_regtype(\n" +
                "  uid int,\n" +
                "  appkey string,\n" +
                "  appregurl string,\n" +
                "  bdp_uuid string,\n" +
                "  createtime timestamp,\n" +
                "  isranreg string,\n" +
                "  regsource string,\n" +
                "  regsourcename string,\n" +
                "  websiteid int,\n" +
                "  dt string)\n" +
                "  partitioned by(dt)");

        //dwd_base_ad
        tableEnv.executeSql("create table iceberg.dwd_base_ad(\n" +
                "adid int,\n" +
                "adname string,\n" +
                "dn string)\n" +
                "partitioned by (dn) ");

        //dwd_base_website
        tableEnv.executeSql(" create table iceberg.dwd_base_website(\n" +
                "  siteid int,\n" +
                "  sitename string,\n" +
                "  siteurl string,\n" +
                " `delete` int,\n" +
                "  createtime timestamp,\n" +
                "  creator string,\n" +
                "  dn string)\n" +
                "partitioned by (dn) ");

        //dwd_pcentermempaymoney
        tableEnv.executeSql("create table iceberg.dwd_pcentermempaymoney(\n" +
                "  uid int,\n" +
                "  paymoney string,\n" +
                "  siteid int,\n" +
                "  vip_id int,\n" +
                "  dt string,\n" +
                "  dn string)\n" +
                " partitioned by(dt,dn)");

        //dwd_vip_level
        tableEnv.executeSql(" create table iceberg.dwd_vip_level(\n" +
                "   vip_id int,\n" +
                "   vip_level string,\n" +
                "   start_time timestamp,\n" +
                "   end_time timestamp,\n" +
                "   last_modify_time timestamp,\n" +
                "   max_free string,\n" +
                "   min_free string,\n" +
                "   next_level string,\n" +
                "   operator string,\n" +
                "   dn string)\n" +
                " partitioned by(dn)");

        //dws_member
        tableEnv.executeSql("create table iceberg.dws_member(\n" +
                "  uid int,\n" +
                "  ad_id int,\n" +
                "  fullname string,\n" +
                "  iconurl string,\n" +
                "  lastlogin string,\n" +
                "  mailaddr string,\n" +
                "  memberlevel string,\n" +
                "  password string,\n" +
                "  paymoney string,\n" +
                "  phone string,\n" +
                "  qq string,\n" +
                "  register string,\n" +
                "  regupdatetime string,\n" +
                "  unitname string,\n" +
                "  userip string,\n" +
                "  zipcode string,\n" +
                "  appkey string,\n" +
                "  appregurl string,\n" +
                "  bdp_uuid string,\n" +
                "  reg_createtime string,\n" +
                "  isranreg string,\n" +
                "  regsource string,\n" +
                "  regsourcename string,\n" +
                "  adname string,\n" +
                "  siteid int,\n" +
                "  sitename string,\n" +
                "  siteurl string,\n" +
                "  site_delete string,\n" +
                "  site_createtime string,\n" +
                "  site_creator string,\n" +
                "  vip_id int,\n" +
                "  vip_level string,\n" +
                "  vip_start_time string,\n" +
                "  vip_end_time string,\n" +
                "  vip_last_modify_time string,\n" +
                "  vip_max_free string,\n" +
                "  vip_min_free string,\n" +
                "  vip_next_level string,\n" +
                "  vip_operator string,\n" +
                " dt string,\n" +
                "dn string)\n" +
                "partitioned by(dt,dn)");

        //ads_register_appregurlnum
        tableEnv.executeSql(" create  table iceberg.ads_register_appregurlnum(\n" +
                "  appregurl string,\n" +
                "  num bigint,\n" +
                "  dt string,\n" +
                "  dn string)\n" +
                "partitioned by(dt)");

        //ads_register_top3memberpay
        tableEnv.executeSql(" create table iceberg.ads_register_top3memberpay(\n" +
                "  uid int,\n" +
                "  memberlevel string,\n" +
                "  register string,\n" +
                "  appregurl string,\n" +
                "  regsourcename string,\n" +
                "  adname string,\n" +
                "  sitename string,\n" +
                "  vip_level string,\n" +
                "  paymoney decimal(10,4),\n" +
                "  rownum BigInt,\n" +
                "dn string,\n" +
                " dt string)\n" +
                "partitioned by(dt,rownum,memberlevel)");
    }

}
