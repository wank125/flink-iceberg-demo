package com.atguigu.iceberg.warhouse.controller;

import com.atguigu.iceberg.warhouse.service.DwdIcebergSerivce;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 将ODS数据抽象成IcerBerg表
 */
public class DwdIcebergController {
    public static DwdIcebergSerivce dwdIcebergSerivce;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        dwdIcebergSerivce = new DwdIcebergSerivce();

        // 测试数据存放路径,需要把测试数据下面的数据全部拷贝到/tmp/warehouse/ods/下面；
        String basePath = "file:///tmp/warehouse/ods/";

        //使用Hadoop本地路径，Icerberg数据保存到/tmp/warehouse/iceberg/iceberg/；
        String catalogPath = "file:///tmp/warehouse/iceberg/iceberg/";

        dwdIcebergSerivce.readOdsData(env, basePath, catalogPath);
        env.execute();
    }
}
