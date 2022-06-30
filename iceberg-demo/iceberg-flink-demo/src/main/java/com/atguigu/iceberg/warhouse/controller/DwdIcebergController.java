package com.atguigu.iceberg.warhouse.controller;

import com.atguigu.iceberg.warhouse.service.DwdIcebergSerivce;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwdIcebergController {
    public static DwdIcebergSerivce dwdIcebergSerivce;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        dwdIcebergSerivce = new DwdIcebergSerivce();
        dwdIcebergSerivce.readOdsData(env);
        env.execute();
    }
}
