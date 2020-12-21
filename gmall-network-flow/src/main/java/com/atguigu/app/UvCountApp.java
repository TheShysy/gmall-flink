package com.atguigu.app;

import com.atguigu.bean.UserBehavior;
import com.atguigu.bean.UvCount;
import com.sun.javafx.scene.control.behavior.BehaviorBase;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

/*
 *
 *@Author:shy
 *@Date:2020/12/21 10:35
 *
 */
public class UvCountApp {
    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取文本数据创建流，转换为Javabean，同时提取数据中的时间戳生成WaterMark
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env.readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new UserBehavior(Long.parseLong(fields[0]),
                                Long.parseLong(fields[1]),
                                Integer.parseInt(fields[2]),
                                fields[3],
                                Long.parseLong(fields[4])
                        );
                    }
                })
                .filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        //开窗
        AllWindowedStream<UserBehavior, TimeWindow> allWindowedStream = userBehaviorDS.timeWindowAll(Time.hours(1));

        //使用全量窗口函数将数据写入HashSet去重
        SingleOutputStreamOperator<UvCount> result = allWindowedStream.apply(new UvCountAllWindowFunc());

        //打印
        result.print();

        //执行
        env.execute();

    }
    public static class UvCountAllWindowFunc implements AllWindowFunction<UserBehavior, UvCount,TimeWindow>{

        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<UvCount> out) throws Exception {
            //创建hashset用于存放用户ID
            HashSet<Object> uids = new HashSet<>();

            //遍历数据  将uid存放在hashset
            Iterator<UserBehavior> iterator = values.iterator();
            while(iterator.hasNext()){
                uids.add(iterator.next().getUserId());
            }
            //输出数据
            out.collect(new UvCount(new Timestamp(window.getEnd()).toString(),(long)uids.size()));
        }
    }
}
