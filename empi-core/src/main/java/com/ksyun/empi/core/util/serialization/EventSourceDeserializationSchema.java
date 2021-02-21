package com.ksyun.empi.core.util.serialization;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Date: 2021/02/07
 * Company: www.ksyun.com
 *
 * @author xuehan
 */
public class EventSourceDeserializationSchema implements KeyedDeserializationSchema<JSONObject> {

    public static Logger logger = LoggerFactory.getLogger(EventSourceDeserializationSchema.class);

    @Override
    public JSONObject deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) {

        JSONObject jsonObject;

        try {
            jsonObject = JSON.parseObject(new String(message), new TypeReference<JSONObject>() {});

            if (jsonObject.containsKey("domain") && jsonObject.containsKey("internalId")) {
                jsonObject.put("data", new String(message));
            } else {
                logger.error("Event {} did not have domain or internalId.", new String(message));
                return null;
            }

        } catch (Exception e) {
            logger.error("Event {} did not have valid JSON content.", new String(message));
            return null;
        }

        return jsonObject;
    }

    @Override
    public boolean isEndOfStream(JSONObject nextElement) {
        return false;
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return TypeInformation.of(new TypeHint<JSONObject>() {});
    }
}
