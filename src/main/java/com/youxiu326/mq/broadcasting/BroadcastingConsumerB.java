package com.youxiu326.mq.broadcasting;

import cn.hutool.core.lang.Console;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class BroadcastingConsumerB {

    public static void main(String[] args) throws MQClientException {

        // 1.创建DefaultMQPushConsumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("demo_producer_broadcasting_group");

        // 2.设置namesrv地址
        consumer.setNamesrvAddr("192.168.211.141:9876");

        // 默认是集群消费模式，改成广播模式
        consumer.setMessageModel(MessageModel.BROADCASTING);

        // 设置消息拉取最大数
        consumer.setConsumeMessageBatchMaxSize(2);

        // 3.设置subscribe,这里是要读取的主题信息
        consumer.subscribe("Topic_Demo", // 指定要消费的主题
                "Tags"             //过滤规则 * 所有; TagesA || TagesA
        );

        // 4.创建消息监听MossageListener
        consumer.setMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                // 5.获取消息信息
                // 迭代消息信息
                for (MessageExt msg : list ) {
                    try {
                        // 获取主题
                        String topic = msg.getTopic();
                        // 获取标签
                        String tags = msg.getTags();

                        // 获取消息信息
                        byte[] body = msg.getBody();
                        String result = new String(body, RemotingHelper.DEFAULT_CHARSET);

                        Console.log("B Consumer消费信息 topic:{},tags:{},result:{}",topic,tags,result);

                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        // 消息重试
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
                // 6.返回消息读取状态
                // 消息消费完成
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });


        // 7.开启Consumer
        consumer.start();

    }

} 