package com.ksyun.empi.core.sink;

import com.ksyun.empi.core.constants.ConstantValue;
import com.ksyun.empi.core.util.HBaseUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;


/**
 * Date: 2021/02/07
 * Company: www.ksyun.com
 *
 * @author xuehan
 */
public class HBaseSinkFunction extends RichSinkFunction<JSONObject> implements CheckpointedFunction {

    private Connection conn;
    private Table table;
    private final Integer maxSize = 500;
    private final Long delayTime = 5000L;
    private Long lastInvokeTime;
    private List<Put> puts = new ArrayList<>();
    private transient ListState<Put> checkpointedState;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        conn = HBaseUtils.getConnection(params.get(ConstantValue.HBASE_ZOOKEEPER_QUORUM), params.get(ConstantValue.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
                , params.get(ConstantValue.HBASE_ZOOKEEPER_ZNODE_PARENT), "3");
        table = conn.getTable(TableName.valueOf(params.get(ConstantValue.HBASE_TABLE)));

        lastInvokeTime = System.currentTimeMillis();
    }

    @Override
    public void close() throws Exception {
        table.close();
        conn.close();
        super.close();
    }

    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {

//        使用MD5Hash可以让HBase上的数据打散，提高写入效率。
//        MD5Hash.getMD5AsHex(Bytes.toBytes("rowkey"));
        Put put1 = new Put(Bytes.toBytes(jsonObject.getString("id")));
        put1.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(jsonObject.getString("domain")),
                Bytes.toBytes(jsonObject.getString("internalId")));

        Put put2 = new Put(Bytes.toBytes(jsonObject.getString("domain") + "_" + jsonObject.getString("internalId")));
        put2.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("globleId"),
                Bytes.toBytes(jsonObject.getString("id")));

        puts.add(put1);
        puts.add(put2);

        long currentTime = System.currentTimeMillis();

        if (puts.size() >= maxSize || currentTime - lastInvokeTime >= delayTime) {

            table.put(puts);
            puts.clear();
            lastInvokeTime = currentTime;
            table.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointedState.clear();
        for (Put put : puts) {
            checkpointedState.add(put);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ListStateDescriptor<Put> descriptor =
                new ListStateDescriptor<>(
                        "buffered-puts",
                        TypeInformation.of(new TypeHint<Put>() {}));

        checkpointedState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);

        if (functionInitializationContext.isRestored()) {
            for (Put put : checkpointedState.get()) {
                puts.add(put);
            }
        }
    }
}
