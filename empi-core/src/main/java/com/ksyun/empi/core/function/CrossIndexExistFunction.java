package com.ksyun.empi.core.function;

import com.alibaba.fastjson.JSONObject;
import com.ksyun.empi.core.constants.ConstantValue;
import com.ksyun.empi.core.util.HBaseUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Date: 2021/02/07
 * Company: www.ksyun.com
 *
 * @author xuehan
 */
public class CrossIndexExistFunction extends RichFilterFunction<JSONObject> {

    private Connection conn;
    private Table table;

    @Override
    public boolean filter(JSONObject jsonObject) throws Exception {

        Get get = new Get(Bytes.toBytes(jsonObject.getString("domain") + "_" + jsonObject.getString("internalId")));
        get.addFamily(Bytes.toBytes("cf"));
        get.setCheckExistenceOnly(true);
        Result result = table.get(get);

        return !result.getExists();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        conn = HBaseUtils.getConnection(params.get(ConstantValue.HBASE_ZOOKEEPER_QUORUM), params.get(ConstantValue.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
                , params.get(ConstantValue.HBASE_ZOOKEEPER_ZNODE_PARENT), "3");
        table = conn.getTable(TableName.valueOf(params.get(ConstantValue.HBASE_TABLE)));
    }

    @Override
    public void close() throws Exception {
        table.close();
        conn.close();
        super.close();
    }
}
