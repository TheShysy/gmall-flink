package com.atguigu.app;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/*
 *
 *@Author:shy
 *@Date:2020/12/22 14:07
 *
 */
public class OrderPayWithCEPAPP {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取文本数据创建流，并转化为JavaBean，并提取时间戳生成watermark
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(Long.parseLong(fields[0]),
                            fields[1],
                            fields[2],
                            Long.parseLong(fields[3]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                });
        //按照orderId进行分类
        KeyedStream<OrderEvent, Long> keyedStream = orderEventDS.keyBy(OrderEvent::getOrderId);

        //定义模式序列
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("start").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("follow").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.minutes(15));

        //将模式序列应用在流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedStream, pattern);

        //提取事件，匹配上的和超时的都需要
        SingleOutputStreamOperator<String> result = patternStream.select(new OutputTag<String>("timeOut") {
                                                                         },
                new MyTimeOutSelect(),
                new MySelectFunc());

        result.print("Result");
        result.getSideOutput(new OutputTag<String>("timeOut") {
        }).print("TimeOut");

        //执行
        env.execute();

    }
    public static class MyTimeOutSelect implements PatternTimeoutFunction<OrderEvent,String>{

        @Override
        public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
            OrderEvent start = pattern.get("start").get(0);
            return start.getOrderId() + "超时！！！";
        }
    }
    public static class MySelectFunc implements PatternSelectFunction<OrderEvent,String> {

        @Override
        public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
            OrderEvent start = pattern.get("start").get(0);
            OrderEvent follow = pattern.get("follow").get(0);
            return start.getOrderId() + " Create at " + start.getEventTime() + ", Payed at " + follow.getEventTime();
        }
    }
}
