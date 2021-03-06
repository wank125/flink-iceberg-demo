package com.atguigu.iceberg.warhouse.controller;

import com.atguigu.iceberg.warhouse.service.DwsIcebergService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsIcebergController {
    public static DwsIcebergService dwsIcebergService;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        dwsIcebergService = new DwsIcebergService();
        dwsIcebergService.getDwsMemberData(env, tableEnv, "20190722");
        env.execute();
    }
}
