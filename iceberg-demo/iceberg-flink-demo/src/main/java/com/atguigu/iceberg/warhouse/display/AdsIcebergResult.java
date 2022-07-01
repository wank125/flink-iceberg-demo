package com.atguigu.iceberg.warhouse.display;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

import static org.apache.flink.table.api.Expressions.$;

public class AdsIcebergResult {
    public static void main(String[] args) throws Exception {

        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String catalogPath = "file:///tmp/warehouse/iceberg/iceberg/";
        TableLoader tableLoader = TableLoader.fromHadoopTable(catalogPath + "ads_register_top3memberpay");
        DataStream<RowData> result = FlinkSource.forRowData().env(env).tableLoader(tableLoader).streaming(false).build();

        Table table = tableEnv.fromDataStream(result);
        Table select = table
                .where($("memberlevel").isEqual("1"))
                .groupBy($("memberlevel"), $("rownum"))
                .select($("uid").min(), $("memberlevel"), $("paymoney").max(), $("rownum"));

        //可撤回的流，高版本统一为Changelog
        DataStream<Tuple2<Boolean, RowData>> tuple2DataStream = tableEnv.toRetractStream(select, RowData.class);

        //过滤掉撤回的
        DataStream<RowData> resultDs = tuple2DataStream.filter(item -> item.f0).map(item -> item.f1);
        resultDs.print();
        env.execute();
    }
}
