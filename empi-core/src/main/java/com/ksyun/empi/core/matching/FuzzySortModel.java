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
public class FuzzySortModel extends SortModel{


    @Override
    public void getResult(MatchingInstance matchingInstance) throws IOException {

        JSONObject jsonObject = matchingInstance.getJsonObject();
        JSONObject config = jsonObject.getJSONObject("config");

        if (config.containsKey("algo")) {
            matchingInstance.setSortModel(new AlgorithmSortModel());
            matchingInstance.getResult();
            return;
        }

        QueryBuilder queryBuilder = getQueryBuilder(jsonObject);
        SearchHit[] searchHits = ElasticsearchUtils.getHits(matchingInstance.getClient(), queryBuilder, 50,
                matchingInstance.getParams().get(ConstantValue.ELASTICSEARCH_INDEX),
                matchingInstance.getParams().get(ConstantValue.ELASTICSEARCH_TYPE));

        if (searchHits.length == 0) {

            return;
        }

        double maxScore = 0;
        String id = "";

        for (SearchHit hit : searchHits) {

            double score = getScore(hit, jsonObject, config);

            if (score > maxScore && score >= Double.parseDouble(config.getString("scoreLine"))) {
                maxScore = score;
                id = hit.getId();
            }
        }

        jsonObject.put("id", id);
        matchingInstance.setJsonObject(jsonObject);
    }

    public static QueryBuilder getQueryBuilder(JSONObject jsonObject) {

        JSONObject config = jsonObject.getJSONObject("config");

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (String key: ConstantValue.ROUGH_SORT.split(",")) {
            if (jsonObject.containsKey(key) && !StringUtils.isEmpty(jsonObject.getString(key))) {
                boolQueryBuilder.should(QueryBuilders.termQuery(key, jsonObject.getString(key)));
            }
        }

        boolQueryBuilder.minimumShouldMatch(config.getString("percent"));


        return QueryBuilders.boolQuery().filter(boolQueryBuilder);
    }

    public static double getScore(SearchHit hit, JSONObject jsonObject, JSONObject config) {

        double score = 0;
        JSONObject hitJsonObject = JSONObject.parseObject(hit.toString()).getJSONObject("_source");

        System.out.println("**********精排打分**********");
        System.out.println(hitJsonObject.toString());

        for (String key : ConstantValue.PERSON_INFO.split(",")) {

            if (hitJsonObject.containsKey(key) && jsonObject.containsKey(key)
                    && hitJsonObject.getString(key).equals(jsonObject.getString(key))) {
                score += Double.parseDouble(config.getString(key));
            }

        }

        System.out.println(score);
        return score;
    }
}
