package com.ksyun.empi.core.matching;


import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import lombok.ToString;
import org.apache.flink.api.java.utils.ParameterTool;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * Date: 2021/02/07
 * Company: www.ksyun.com
 *
 * @author xuehan
 */
@Data
@ToString
public class MatchingInstance {

    private SortModel sortModel;
    private JSONObject jsonObject;
    private RestHighLevelClient client;
    private ParameterTool params;

    public MatchingInstance(JSONObject jsonObject, RestHighLevelClient client, ParameterTool params) {
        sortModel = new ExactlySortModel();
        this.jsonObject = jsonObject;
        this.client = client;
        this.params = params;
    }


    public void getResult() throws IOException {
        sortModel.getResult(this);
    }
}
