package com.youxiu326.mq.transaction;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;

public class TransactionProducer {

    public static void main(String[] args) throws UnsupportedEncodingException, InterruptedException, RemotingException, MQClientException, MQBrokerException {

        // 1.创建DefaultMQProducer
        TransactionMQProducer producer = new TransactionMQProducer(
                "demo_producer_transaction_group");//producerGroup

        // 2.设置Namesrv地址
        producer.setNamesrvAddr("192.168.211.141:9876");

        TransactionListener transactionListener = new  TransactionListenerImpl();
        // 指定消息监听对象，用于执行本地事务和消息回查
        producer.setTransactionListener(transactionListener);

        // 线程池
        ExecutorService executorService = new ThreadPoolExecutor(
                2,
                5,
                100,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(200),
                new ThreadFactory(){
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("client-transaction-msg-check-thread");
                        return thread;
                    }
                }
        );

        producer.setExecutorService(executorService);

        // 3.开启创建DefaultMQProducer
        producer.start();

        // 4.创建消息Message
        Message message = new Message(
                "Topic_Transaction_Demo",   // 主题
                "Tags",         // 主要用于消息过滤
                "Key_1",        // 消息的唯一值
                "hello world".getBytes(RemotingHelper.DEFAULT_CHARSET)
        );

        // 5.发送事务消息
        TransactionSendResult result = producer.sendMessageInTransaction(message, "hello-transaction");

        System.out.println(result);

        // 6.关闭
        producer.shutdown();


    }


} 