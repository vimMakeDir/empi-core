package com.ksyun.empi.core.matching;

import com.alibaba.fastjson.JSONObject;
import com.ksyun.empi.core.constants.ConstantValue;
import com.ksyun.empi.core.util.ElasticsearchUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;


/**
 * Date: 2021/02/07
 * Company: www.ksyun.com
 *
 * @author xuehan
 */
public class ExactlySortModel extends SortModel {

    @Override
    public void getResult(MatchingInstance matchingInstance) throws IOException {

        JSONObject jsonObject = matchingInstance.getJsonObject();
        JSONObject config = jsonObject.getJSONObject("config");
        String exactString = "";

        for (String str:config.getString("exact").split(",")) {
            if (jsonObject.containsKey(str) && !StringUtils.isEmpty(jsonObject.getString(str))) {
                exactString = str;
                break;
            }
        }

        if (!StringUtils.isEmpty(exactString)) {
            QueryBuilder queryBuilder = getQueryBuilder(exactString, jsonObject.getString(exactString));

            SearchHit[] searchHits = ElasticsearchUtils.getHits(matchingInstance.getClient(), queryBuilder, 10,
                    matchingInstance.getParams().get(ConstantValue.ELASTICSEARCH_INDEX),
                    matchingInstance.getParams().get(ConstantValue.ELASTICSEARCH_TYPE));

            if (searchHits.length != 0) {
                jsonObject.put("id", searchHits[0].getId());
            }

            matchingInstance.setJsonObject(jsonObject);
        } else {
            matchingInstance.setSortModel(new FuzzySortModel());
            matchingInstance.getResult();
        }
    }

    public static QueryBuilder getQueryBuilder (String type, String value) {

        return QueryBuilders.constantScoreQuery(new BoolQueryBuilder().should(QueryBuilders.termQuery(type, value)));
    }
}
