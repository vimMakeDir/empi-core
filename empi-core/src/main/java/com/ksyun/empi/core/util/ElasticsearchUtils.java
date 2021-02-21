package com.ksyun.empi.core.util;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Date: 2021/02/07
 * Company: www.ksyun.com
 *
 * @author xuehan
 */
public class ElasticsearchUtils {

    public static RestHighLevelClient getClient(String address) {

        List<HttpHost> httpHostList = new ArrayList<>();
        String[] addr = address.split(",");
        for(String add : addr) {
            String[] pair = add.split(":");
            TelnetUtil.telnet(pair[0], Integer.parseInt(pair[1]));
            httpHostList.add(new HttpHost(pair[0], Integer.parseInt(pair[1]), "http"));
        }

        return new RestHighLevelClient(RestClient.builder(httpHostList.toArray(new HttpHost[0])));
    }

    public static SearchHit[] getHits(RestHighLevelClient client, QueryBuilder queryBuilder, Integer size, String index, String type) throws IOException {

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);
        sourceBuilder.from(0);
        sourceBuilder.size(size);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);

        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();

        SearchHit[] searchHits = hits.getHits();

        return searchHits;
    }
}
