package com.ksyun.empi.core.function;

import com.ksyun.empi.core.constants.ConstantValue;
import com.ksyun.empi.core.matching.MatchingInstance;
import com.ksyun.empi.core.util.ElasticsearchUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.elasticsearch.client.RestHighLevelClient;


/**
 * Date: 2021/02/07
 * Company: www.ksyun.com
 *
 * @author xuehan
 */
public class MatchingFunction extends RichMapFunction<JSONObject, JSONObject> {

    private ParameterTool params;
    private RestHighLevelClient client;

    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {

        long startTime = System.currentTimeMillis();
        MatchingInstance matchingInstance = new MatchingInstance(jsonObject, client, params);
        matchingInstance.getResult();

        System.out.println("******算法耗时******");
        System.out.println(System.currentTimeMillis() - startTime);

        return matchingInstance.getJsonObject();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        client = ElasticsearchUtils.getClient(params.get(ConstantValue.ELASTICSEARCH_SERVERS));
    }

    @Override
    public void close() throws Exception {
        client.close();
        super.close();
    }
}
