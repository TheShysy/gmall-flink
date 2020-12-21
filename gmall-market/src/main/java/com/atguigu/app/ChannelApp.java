package com.atguigu.app;

import com.atguigu.bean.ChannelBehaviorCount;
import com.atguigu.bean.MarketUserBehavior;
import com.atguigu.source.MarketBehaviorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/*
 *
 *@Author:shy
 *@Date:2020/12/21 14:10
 *
 */
public class ChannelApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从自定义的数据源获取数据，并转化为JavaBean
        DataStreamSource<MarketUserBehavior> marketUserBehaviorDS = env.addSource(new MarketBehaviorSource());

        //按照渠道进行分组
        KeyedStream<MarketUserBehavior, Tuple> keyedStream = marketUserBehaviorDS.keyBy("channel", "behavior");

        //开窗、滑动窗口、滑动步长5分钟，窗口大小一个小时
        WindowedStream<MarketUserBehavior, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.hours(1), Time.seconds(5));

        //使用aggregate实现累加聚合以及添加窗口信息
        SingleOutputStreamOperator<ChannelBehaviorCount> result = windowedStream.aggregate(new ChannelAggFunc(), new ChannelWindowFunc());

        //打印结果
        result.print();

        //执行任务
        env.execute();
    }
    public static class ChannelAggFunc implements AggregateFunction<MarketUserBehavior,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketUserBehavior marketUserBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }
    public static class ChannelWindowFunc implements WindowFunction<Long, ChannelBehaviorCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ChannelBehaviorCount> out) throws Exception {
            //取出channel
            String channel = tuple.getField(0);
            //取出行为
            String behavior = tuple.getField(1);
            //取出窗口结束时间
            String windowEnd = new Timestamp(window.getEnd()).toString();
            //取出总的数量
            Long count = input.iterator().next();
            //输出
            out.collect(new ChannelBehaviorCount(channel,behavior,windowEnd,count));
        }
    }
}
