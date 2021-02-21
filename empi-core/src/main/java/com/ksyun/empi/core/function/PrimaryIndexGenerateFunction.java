package com.ksyun.empi.core.function;

import com.ksyun.empi.core.constants.ConstantValue;
import com.ksyun.empi.core.util.ElasticsearchUtils;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;


/**
 * Date: 2021/02/07
 * Company: www.ksyun.com
 *
 * @author xuehan
 */
@Slf4j
public class PrimaryIndexGenerateFunction extends RichMapFunction<JSONObject, JSONObject> {

    private ParameterTool params;
    private RestHighLevelClient client;

    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {

        long startTime = System.currentTimeMillis();
        JSONObject tmp = new JSONObject();
        JSONObject object = jsonObject.getJSONObject("data");

        for (String key: ConstantValue.PERSON_INFO.split(",")) {
            tmp.put(key, object.getString(key));
        }

        IndexRequest request = new IndexRequest(params.get(ConstantValue.ELASTICSEARCH_INDEX), params.get(ConstantValue.ELASTICSEARCH_TYPE));
        request.source(tmp.toJSONString(), XContentType.JSON);

        IndexResponse indexResponse = null;

        try {
            indexResponse = client.index(request);
        } catch(ElasticsearchException e) {
            System.out.println("Elasticsearch 抛错: " + e.toString());
            if (e.status() == RestStatus.CONFLICT) {
                log.error("Elasticsearch写入冲突: " + e.getDetailedMessage());
            }
        }

        System.out.println("写入es耗时: " + (System.currentTimeMillis() - startTime));

        JSONObject result = new JSONObject();
        result.put("id", indexResponse.getId());
        result.put("domain", object.getString("domain"));
        result.put("internalId", object.getString("internalId"));
        result.put("data", object.toString());

        return result;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        client = ElasticsearchUtils.getClient(params.get(ConstantValue.ELASTICSEARCH_SERVERS));
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
