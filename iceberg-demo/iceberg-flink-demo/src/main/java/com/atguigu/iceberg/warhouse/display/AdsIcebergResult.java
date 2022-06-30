package com.atguigu.iceberg.warhouse.display;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
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

        tableEnv.createTemporaryView("T1", result);
        tableEnv.executeSql("SELECT * FROM T1 LIMIT 10").print();
        //Table table = tableEnv.sqlQuery("select max(uid),memberlevel,max(paymoney),rownum from T1 where memberlevel='1'  group by memberlevel,rownum");
        //TableResult tableResult = tableEnv.executeSql("select max(uid),memberlevel,max(paymoney),rownum from T1 where memberlevel='1'  group by memberlevel,rownum");
        //tableResult.print();
        // tableEnv.toRetractStream(table, RowData.class).print();

//        Table select = table
//                .groupBy($("memberlevel"), $("rownum"))
//                .select($("uid").min(), $("memberlevel"), $("paymoney").max(), $("rownum"));
//        Table uid = table.select($("uid"));
//        tableEnv.toRetractStream(uid, RowData.class).print();
    }
}
