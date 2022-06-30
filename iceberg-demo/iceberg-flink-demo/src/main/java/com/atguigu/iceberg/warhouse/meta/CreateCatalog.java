package com.atguigu.iceberg.warhouse.meta;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 创建Hadoop Catalog
 */
public class CreateCatalog {
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

        // tableEnv.executeSql("CREATE DATABASE iceberg");

        tableEnv.executeSql("USE  iceberg");

        tableEnv.executeSql("CREATE TABLE  events (\n" +
                "    `user` STRING,\n" +
                "     url STRING,\n" +
                "    `timestamp` BIGINT  \n" +
                ")");
    }
}
