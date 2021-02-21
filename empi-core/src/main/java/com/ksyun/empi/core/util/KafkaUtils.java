package com.ksyun.empi.core.util;

import com.ksyun.empi.core.constants.ConstantValue;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Properties;

public class KafkaUtils {

    public static Properties CreateProperties (ParameterTool params) {

        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConstantValue.BOOTSTRAP_SERVERS, params.get(ConstantValue.BOOTSTRAP_SERVERS));
        consumerProps.setProperty(ConstantValue.GROUP_ID, params.get(ConstantValue.GROUP_ID));
        consumerProps.setProperty("auto.offset.reset", "latest");

        return consumerProps;
    }
}
