package com.youxiu326.mq.quickstart;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

public class Producer {

    public static void main(String[] args) throws UnsupportedEncodingException, InterruptedException, RemotingException, MQClientException, MQBrokerException {

        // 1.创建DefaultMQProducer
        DefaultMQProducer producer = new DefaultMQProducer(
                "demo_producer_group");//producerGroup

        // 2.设置Namesrv地址
        producer.setNamesrvAddr("192.168.211.141:9876");

        // 3.开启创建DefaultMQProducer
        producer.start();

        // 4.创建消息Message
        Message message = new Message(
                "Topic_Demo",   // 主题
                "Tags",         // 主要用于消息过滤
                "Key_1",        // 消息的唯一值
                "hello world".getBytes(RemotingHelper.DEFAULT_CHARSET)
        );

        // 5.发送消息
        SendResult result = producer.send(message);

        // 6.关闭
        producer.shutdown();


    }


} 