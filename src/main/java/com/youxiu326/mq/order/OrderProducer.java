package com.youxiu326.mq.order;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class OrderProducer {

    public static void main(String[] args) throws UnsupportedEncodingException, InterruptedException, RemotingException, MQClientException, MQBrokerException {

        // 1.创建DefaultMQProducer
        DefaultMQProducer producer = new DefaultMQProducer(
                "demo_producer_order_group");//producerGroup

        // 2.设置Namesrv地址
        producer.setNamesrvAddr("192.168.211.141:9876");

        // 3.开启创建DefaultMQProducer
        producer.start();

        // 4.创建消息Message
        Message message = new Message(
                "Topic_Order_Demo",   // 主题
                "Tags",         // 主要用于消息过滤
                "Key_1",        // 消息的唯一值
                "hello world".getBytes(RemotingHelper.DEFAULT_CHARSET)
        );

        // 5.发送顺序消息
        // 第1个参数：发送的消息信息
        // 第2个参数：选中指定的消息队列队形(会将所有的消息队列传入进来)
        // 第3个参数：指定对应的队列下标
        SendResult result = producer.send(message,
                new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object arg) {
                        // 获取队列的下标
                        Integer index = (Integer) arg;
                        // 获取对应下标的队列
                        return list.get(index);
                    }
                },
                1); // 指定发送到下标为1的队列

        // 6.关闭
        producer.shutdown();


    }


} 