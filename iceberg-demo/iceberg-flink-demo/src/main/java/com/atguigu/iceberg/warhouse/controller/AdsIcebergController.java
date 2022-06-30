package com.atguigu.iceberg.warhouse.controller;

import com.atguigu.iceberg.warhouse.service.AdsIcebergService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class AdsIcebergController {
    public static AdsIcebergService adsIcebergService=new AdsIcebergService();
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        adsIcebergService.queryDetails(env,tableEnv,"20190722");
        env.execute();
    }
}
