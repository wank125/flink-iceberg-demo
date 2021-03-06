package com.atguigu.iceberg.warhouse.dao;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

import static org.apache.flink.table.api.Expressions.$;

public class DwsIcbergDao {

    private String catalogPath = "file:///tmp/warehouse/iceberg/iceberg/";

    public Table queryDwsMemberData(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        TableLoader tableLoader = TableLoader.fromHadoopTable(catalogPath + "dws_member");
        DataStream<RowData> result = FlinkSource.forRowData().env(env).tableLoader(tableLoader).streaming(false).build();
        Table table = tableEnv.fromDataStream(result).select($("uid"), $("ad_id"), $("memberlevel"),
                $("register"), $("appregurl"), $("regsource"), $("regsourcename"),
                $("adname"), $("sitename"), $("vip_level"), $("paymoney").cast(DataTypes.DECIMAL(10, 4)).as("paymoney"),
                $("dt"), $("dn"));

        return table;
    }
}
