package com.youxiu326.mq.transaction;

import cn.hutool.core.lang.Console;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.sql.SQLOutput;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionListenerImpl implements TransactionListener {

    // 存储对应事务的状态信息 key：事务ID,value：当前事务执行的状态
    private ConcurrentHashMap<String,Integer> localTrans = new ConcurrentHashMap<>();

    /**
     * 执行本地事务
     * @param message
     * @param o
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {

        // 事务ID
        String transactionId = message.getTransactionId();

        // 业务执行，处理本地事务 service
        //0:执行中     1:本地事务执行成功      2.本地事务执行失败

        try {
            localTrans.put(transactionId, 0);
            Console.log("正在执行本地事务--");
            Thread.sleep(1000*60+1000);
            Console.log("正在执行本地事务--成功！");
            localTrans.put(transactionId, 1);

        } catch (InterruptedException e) {
            e.printStackTrace();
            localTrans.put(transactionId, 2);
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }

        return LocalTransactionState.COMMIT_MESSAGE;
    }

    /**
     * 1分钟后会执行 消息回查
     * @param messageExt
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {

        // 事务ID
        String transactionId = messageExt.getTransactionId();

        Integer status = localTrans.get(transactionId);

        Console.log("消息回查---transactionId:{},status={}",transactionId,status);

        switch (status){
            case 0:
                return LocalTransactionState.UNKNOW;
            case 1:
                return LocalTransactionState.COMMIT_MESSAGE;
            case 2:
                return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        return LocalTransactionState.UNKNOW;
    }
}