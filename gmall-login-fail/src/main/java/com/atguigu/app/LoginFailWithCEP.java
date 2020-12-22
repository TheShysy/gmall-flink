package com.atguigu.app;

import com.atguigu.bean.LoginEvent;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/*
 *
 *@Author:shy
 *@Date:2020/12/21 18:50
 *
 */
public class LoginFailWithCEP {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取文本数据并转换为JavaBean，提取数据中的时间戳生成watermark
        SingleOutputStreamOperator<LoginEvent> loginEventDS = env.readTextFile("input/LoginLog.csv")
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String s) throws Exception {
                        String[] fields = s.split(",");
                        return new LoginEvent(Long.parseLong(fields[0]),
                                fields[1],
                                fields[2],
                                Long.parseLong(fields[3]));
                    }
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //按照用户id分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventDS.keyBy(LoginEvent::getUserId);

        //定义模式序列连续两次失败
//        Pattern<LoginEvent,LoginEvent> pattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
//            @Override
//            public boolean filter(LoginEvent value) throws Exception {
//                return "fail".equals(value.getEventType());
//            }
//        }).next("next").where(new SimpleCondition<LoginEvent>() {
//            @Override
//            public boolean filter(LoginEvent value) throws Exception {
//                return "fail".equals(value.getEventType());
//            }
//        }).within(Time.seconds(2));

        //使用循环方法
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getEventType());
            }
        }).times(5)  //默认使用的是非严格近邻
                .consecutive()  //严格近邻
                .within(Time.seconds(5));

        //将模式序列作用到流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, pattern);

        //提取事件
        SingleOutputStreamOperator<String> result = patternStream.select(new MyPatternSelectFunc());


        //打印结果
        result.print();

        //执行任务
        env.execute();

    }

    public static class MyPatternSelectFunc implements PatternSelectFunction<LoginEvent, String> {

        @Override
        public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
//            //提取事件
//            LoginEvent start = pattern.get("start").get(0);
//            LoginEvent next = pattern.get("next").get(0);
            List<LoginEvent> events = pattern.get("start");
            LoginEvent start = events.get(0);
            LoginEvent next = events.get(events.size() - 1);

            return start.getUserId() + "在" + start.getTimestamp() + "到" + next.getTimestamp() + "之间连续登录失败" + events.size() + "次";
        }
    }


}
