package com.atguigu.app;

import com.atguigu.bean.LoginEvent;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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

/*
 *
 *@Author:shy
 *@Date:2020/12/21 18:50
 *
 */
public class LoginFailWithState1 {
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

        //使用ProcessFuction处理数据
        SingleOutputStreamOperator<String> result = keyedStream.process(new LoginFailProcessFunc(2));

        //打印结果
        result.print();

        //执行任务
        env.execute();

    }

    public static class LoginFailProcessFunc extends KeyedProcessFunction<Long, LoginEvent, String> {

        //定义属性
        private int interval;

        public LoginFailProcessFunc(int interval) {
            this.interval = interval;
        }

        //定义状态
        private ValueState<LoginEvent> failEventState;

        @Override
        public void open(Configuration parameters) throws Exception {
            failEventState = getRuntimeContext().getState(new ValueStateDescriptor<LoginEvent>("list-state", LoginEvent.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {

            //如果当前数据为失败数据
            if ("fail".equals(value.getEventType())) {

                //取出状态数据
                LoginEvent lastFail = failEventState.value();
                failEventState.update(value);

                //非第一次数据  比较时间间隔
                if (lastFail != null && Math.abs(value.getTimestamp() - lastFail.getTimestamp()) <= interval) {
                    //输出报警信息
                    out.collect(value.getUserId() + "连续登录失败两次！！");
                }
            } else {
                //清空状态
                failEventState.clear();
            }
        }


    }
}
