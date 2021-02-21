package com.ksyun.empi.core.function;

import com.alibaba.fastjson.JSONObject;
import com.ksyun.empi.core.Launcher;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;


/**
 * Date: 2021/02/07
 * Company: www.ksyun.com
 *
 * @author xuehan
 */
public class ConnectedBroadcastProcessFuntion extends KeyedBroadcastProcessFunction<String, JSONObject, JSONObject, JSONObject> {

    public static Logger logger = LoggerFactory.getLogger(ConnectedBroadcastProcessFuntion.class);

    private JSONObject defaultConfig = JSONObject.parseObject("{\"name\":\"50\",\"date\":\"50\",\"sex\":\"50\",\"national\":\"50\"," +
            "\"tel\":\"50\",\"handset\":\"50\",\"scoreLine\":\"250\",\"hra\":\"50\",\"ra\":\"50\",\"percent\":\"60%\", \"exact\":\"idcard,sscn\"}");

    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

        JSONObject config = readOnlyContext.getBroadcastState(Launcher.configStateDescriptor).get(jsonObject.getString("domain"));

        if (Objects.isNull(config)) {
            config = defaultConfig;
        }

        jsonObject.put("config", config);
        collector.collect(jsonObject);
    }

    @Override
    public void processBroadcastElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {

        BroadcastState<String, JSONObject> state = context.getBroadcastState(Launcher.configStateDescriptor);
        state.put(jsonObject.getString("domain"), jsonObject);
        logger.info("读取配置: {}.", new String(jsonObject.toString()));
    }
}
