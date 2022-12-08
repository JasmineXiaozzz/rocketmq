/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.quickstart.learning;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerTest {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(ConsumerTest.class);
    private static final DefaultMQPushConsumer consumer;
//    private static final DefaultMQPushConsumer consumer1;

    static {
        consumer = new DefaultMQPushConsumer(MQConstants.CONSUMER_GROUP,true);
        consumer.setNamesrvAddr(MQConstants.NAMESRVADDR);
        /*
         * 指定从哪里开始，以防特定的消费群体是一个全新的。
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumeMessageBatchMaxSize(1);
        /*
         * 默认集群消费, 广播消费使用 MessageModel.BROADCASTING
         */
        consumer.setMessageModel(MessageModel.CLUSTERING);
        try {
             consumer.subscribe("topic_learning_test", "*");
//            consumer.subscribe("TRANS_CHECK_MAX_TIME_TOPIC", "*");
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        // 两个消费者
//        consumer1 = new DefaultMQPushConsumer(MQConstants.CONSUMER_GROUP,true);
//        consumer1.setNamesrvAddr(MQConstants.NAMESRVADDR);
//        /*
//         * 指定从哪里开始，以防特定的消费群体是一个全新的。
//         */
//        consumer1.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
//        consumer1.setConsumeMessageBatchMaxSize(1);
//        /*
//         * 默认集群消费, 广播消费使用 MessageModel.BROADCASTING
//         */
//        consumer1.setMessageModel(MessageModel.CLUSTERING);
//        try {
//            // consumer.subscribe(MQConstants.TOPIC, "*");
//            consumer1.subscribe("TRANS_CHECK_MAX_TIME_TOPIC", "*");
//        } catch (MQClientException e) {
//            e.printStackTrace();
//        }
    }

    public static void main(String[] args) {
//        push();
        // 接收顺序消息
        pushOrder();

        // 拉取消息
        // litePull();
    }

    /**
     * 推的方式获取消息, 实际原理上也是客户端主动拉取
     */
    public static void push() {

        // 每个客户端只能注册一个消息监听
        // 如果注册的是 MessageListenerConcurrently，那该Consumer就是并发消费
        // 如果注册的是 MessageListenerOrderly，那该Consumer就是顺序消费
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            /**
             * 消费消息
             * @param msgs 消息集合, 可能会存在多条消息, 如果需要消费一条消息, 需要设置
             *             {@link DefaultMQPushConsumer#setConsumeMessageBatchMaxSize(int)} = 1
             * @param context 上下文
             * @return 是否消费成功
             */
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                log.info("收到消息: {}", new String(msgs.get(0).getBody(), StandardCharsets.UTF_8));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

//        consumer1.registerMessageListener(new MessageListenerConcurrently() {
//
//            /**
//             * 消费消息
//             * @param msgs 消息集合, 可能会存在多条消息, 如果需要消费一条消息, 需要设置
//             *             {@link DefaultMQPushConsumer#setConsumeMessageBatchMaxSize(int)} = 1
//             * @param context 上下文
//             * @return 是否消费成功
//             */
//            @Override
//            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
//                log.info("收到消息: {}", new String(msgs.get(0).getBody(), StandardCharsets.UTF_8));
//                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//            }
//        });


        try {
            consumer.start();
//            consumer1.start();
            System.out.println("消费方已按照 [push] 推方式启动！");
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        ScheduledThreadPoolExecutor scheduled = new ScheduledThreadPoolExecutor(1);

        scheduled.scheduleWithFixedDelay(() -> {
            ConsumeStatus consumeStatus = consumer.getDefaultMQPushConsumerImpl().getConsumerStatsManager()
                    .consumeStatus("topic_learning_test", MQConstants.CONSUMER_GROUP);
            System.out.println("拉取平均用时： "+ consumeStatus.getConsumeRT());
            System.out.println("每秒拉取数量： "+consumeStatus.getConsumeOKTPS());
        }, 1000 * 10, 300, TimeUnit.MILLISECONDS);



    }

    /**
     * 顺序消费模式
     */
    public static void pushOrder() {
        consumer.registerMessageListener(new MessageListenerOrderly() {

            private AtomicInteger integer = new AtomicInteger();

            /**
             * {@link ConsumeOrderlyStatus#SUCCESS}: 消费成功
             * {@link ConsumeOrderlyStatus#SUSPEND_CURRENT_QUEUE_A_MOMENT}: 暂时挂起当前队列
             * @param msgs 消费集合
             * @param context
             * @return
             */
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                int i = integer.incrementAndGet();
                if (i == 4) {
                    integer.set(0);
                    log.warn("返回队列挂起");
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
                log.info("收到消息: {}", new String(msgs.get(0).getBody(), StandardCharsets.UTF_8));
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        try {
            consumer.start();
            System.out.println("消费方已按照 [push] 推方式 [顺序消费] 启动！");
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }


    /** ====================================================================================================
     * 拉消息
     * ==================================================================================================== */
    //region
    private static final boolean running = true;
    private static final DefaultLitePullConsumer litePullConsumer;
    static {
        litePullConsumer = new DefaultLitePullConsumer(MQConstants.CONSUMER_GROUP);
        litePullConsumer.setNamesrvAddr(MQConstants.NAMESRVADDR);
        try {
            litePullConsumer.subscribe(MQConstants.TOPIC, "*");
            // 一次拉取的最大最大条数
            litePullConsumer.setPullBatchSize(20);
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    public static void litePull() {
        System.out.println("消费方已按照 [pull] 拉方式启动！");
        try {
            litePullConsumer.start();
            while (running) {
                List<MessageExt> messageExts = litePullConsumer.poll();
                for (MessageExt messageExt : messageExts) {
                    log.info("收到消息: {}", new String(messageExt.getBody(), StandardCharsets.UTF_8));
                }
            }
        } catch (MQClientException e) {
            e.printStackTrace();
        } finally {
            litePullConsumer.shutdown();
        }
    }

    // endregion
}
