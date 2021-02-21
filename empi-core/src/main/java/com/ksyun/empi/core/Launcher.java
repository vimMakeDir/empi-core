package com.ksyun.empi.core;

import com.alibaba.fastjson.JSONObject;
import com.ksyun.empi.core.function.*;
import com.ksyun.empi.core.sink.HBaseSinkFunction;
import com.ksyun.empi.core.util.serialization.ConfigEventDeserializationSchema;
import com.ksyun.empi.core.constants.ConstantValue;
import com.ksyun.empi.core.util.serialization.EventSourceDeserializationSchema;
import com.ksyun.empi.core.util.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import static com.ksyun.empi.core.util.ParameterCheck.parameterCheck;


/**
 * Date: 2021/02/07
 * Company: www.ksyun.com
 *
 * @author xuehan
 */
public class Launcher {

    public static Logger logger = LoggerFactory.getLogger(Launcher.class);

    public static final MapStateDescriptor<String, JSONObject> configStateDescriptor =
            new MapStateDescriptor<>(
                    "configBroadcastState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<JSONObject>() {}));

    @SneakyThrows
    public static void main(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = parameterCheck(args);
        env.getConfig().setGlobalJobParameters(params);

        env.enableCheckpointing(60000L);
        CheckpointConfig checkpointConf=env.getCheckpointConfig();
        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConf.setMinPauseBetweenCheckpoints(30000L);
        checkpointConf.setCheckpointTimeout(100000L);
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

//        状态持久化
//        StateBackend backend=new RocksDBStateBackend(
//                "hdfs://hdfs-ha:8020/flink/checkpoints",
//                true);
//
//        env.setStateBackend(backend);
//
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
//                3,
//                Time.of(10, TimeUnit.SECONDS)
//        ));


        Properties consumerProps = KafkaUtils.CreateProperties(params);

        final FlinkKafkaConsumer kafkaEventSource = new FlinkKafkaConsumer<JSONObject>(
                params.get(ConstantValue.INPUT_EVENT_TOPIC),
                new EventSourceDeserializationSchema(), consumerProps);

        final KeyedStream<JSONObject, String> sourceStream = env
                .addSource(kafkaEventSource)
                .keyBy(new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject event) throws Exception {
                        return event.getString("domain");
                    }
                });

        final FlinkKafkaConsumer kafkaConfigEventSource = new FlinkKafkaConsumer<JSONObject>(
                params.get(ConstantValue.INPUT_CONFIG_TOPIC), new ConfigEventDeserializationSchema()
                , consumerProps);

        final BroadcastStream<JSONObject> configBroadcastStream = env
                .addSource(kafkaConfigEventSource)
                .broadcast(configStateDescriptor);

        DataStream<JSONObject> connectedStream = sourceStream.connect(configBroadcastStream).process(new ConnectedBroadcastProcessFuntion());

        DataStream<JSONObject> jsonObjectDataStream = connectedStream.rebalance().filter(new CrossIndexExistFunction())
                .map(new MatchingFunction());

        final OutputTag<JSONObject> failure = new OutputTag<JSONObject>("failure"){};

        SingleOutputStreamOperator<JSONObject> sucessed = jsonObjectDataStream.process(new ProcessFunction<JSONObject, JSONObject>() {

            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {

                if (!StringUtils.isEmpty(jsonObject.getString("id"))) {
                    collector.collect(jsonObject);
                } else if (jsonObject.containsKey("idcard")) {
                    context.output(failure, jsonObject);
                }

            }
        });

        sucessed.rebalance().addSink(new HBaseSinkFunction());
        sucessed.getSideOutput(failure).rebalance().map(new PrimaryIndexGenerateFunction()).addSink(new HBaseSinkFunction());

        env.execute("flink-empi");
    }
}
