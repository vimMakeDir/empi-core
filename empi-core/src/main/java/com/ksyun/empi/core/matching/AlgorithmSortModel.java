package com.ksyun.empi.core.matching;

import com.alibaba.fastjson.JSONObject;
import com.ksyun.empi.core.algorithm.DistanceMetric;
import com.ksyun.empi.core.algorithm.Impl.StringComparisonServiceImpl;
import com.ksyun.empi.core.algorithm.JaroWinklerAliasiDistanceMetric;
import com.ksyun.empi.core.algorithm.LevenshteinDistanceMetric;
import com.ksyun.empi.core.constants.ConstantValue;
import com.ksyun.empi.core.util.ElasticsearchUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.HashMap;


/**
 * Date: 2021/02/08
 * Company: www.ksyun.com
 *
 * @author xuehan
 */
public class AlgorithmSortModel extends SortModel {

    public static StringComparisonServiceImpl stringComparisonService = new StringComparisonServiceImpl();

    static {
        HashMap<String, DistanceMetric> metricTypeMap = new HashMap<>();
        DistanceMetric jaroWinklerAliasiDistanceMetric = new JaroWinklerAliasiDistanceMetric();
        metricTypeMap.put("JaroWinklerAliasiDistance", jaroWinklerAliasiDistanceMetric);
        DistanceMetric levenshteinDistanceMetric = new LevenshteinDistanceMetric();
        metricTypeMap.put("LevenshteinDistance", levenshteinDistanceMetric);
        stringComparisonService.setDistanceMetricTypeMap(metricTypeMap);
    }

    @Override
    public void getResult(MatchingInstance matchingInstance) throws IOException {
        JSONObject jsonObject = matchingInstance.getJsonObject();
        JSONObject config = jsonObject.getJSONObject("config");

        QueryBuilder queryBuilder = FuzzySortModel.getQueryBuilder(jsonObject);
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

            if (score > maxScore && score >= Double.parseDouble(config.getString("percent").split("%")[0]) / 100) {
                maxScore = score;
                id = hit.getId();
            }
        }

        jsonObject.put("id", id);
        matchingInstance.setJsonObject(jsonObject);
    }

    public static double getScore(SearchHit hit, JSONObject jsonObject, JSONObject config) {

        JSONObject hitJsonObject = JSONObject.parseObject(hit.toString()).getJSONObject("_source");

        System.out.println("**********精排打分**********");
        System.out.println(hitJsonObject.toString());

        double score = 0;
        double deno = 0;
        String[] keyArray = ConstantValue.PERSON_INFO.split(",");
        for (String key: keyArray) {
            if (config.containsKey(key) && !StringUtils.isEmpty(config.getString(key))) {
                deno += Double.parseDouble(config.getString(key));
            }
        }

        for (String key: keyArray) {
            if (jsonObject.containsKey(key) && !StringUtils.isEmpty(jsonObject.getString(key)) && hitJsonObject.containsKey(key)
                    && !StringUtils.isEmpty(hitJsonObject.getString(key))) {
                score += ((config.getDouble(key) / deno) * stringComparisonService.score(config.getString("algo"), jsonObject.get(key)
                    , hitJsonObject.getString(key)));
            }
        }

        System.out.println(score);
        return score;
    }
}
