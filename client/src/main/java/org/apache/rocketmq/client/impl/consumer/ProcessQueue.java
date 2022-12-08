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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;

/**
 * Queue consumption snapshot
 * <p>
 * 消耗队列, 每个 ConsumeQueue 在客户端都有个对应的 ProcessQueue, 也相当于一个 ConsumeQueue 快照
 */
public class ProcessQueue {
    public final static long REBALANCE_LOCK_MAX_LIVE_TIME =
            Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
    public final static long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
    private final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * 读写锁 treeMapLock 对消息的操作都要使用
     */
    private final ReadWriteLock treeMapLock = new ReentrantReadWriteLock();
    /**
     * 处理队列的消息缓存，treemap 会根据 key 排序
     * key: 消息的 offset
     * value: 消息
     */
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<Long, MessageExt>();
    /**
     * 消息总数量
     */
    private final AtomicLong msgCount = new AtomicLong();
    /**
     * 整个ProcessQueue处理单元的总消息长度
     */
    private final AtomicLong msgSize = new AtomicLong();

    /**
     * 重入锁 consumeLock
     */
    private final Lock consumeLock = new ReentrantLock();
    /**
     * A subset of msgTreeMap, will only be used when orderly consume
     * <p>一个临时的TreeMap, 仅在顺序消费模式下使用
     */
    private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<Long, MessageExt>();
    private final AtomicLong tryUnlockTimes = new AtomicLong(0);
    /**
     * 整个ProcessQueue处理单元的offset最大边界
     */
    private volatile long queueOffsetMax = 0L;
    private volatile boolean dropped = false;
    private volatile long lastPullTimestamp = System.currentTimeMillis();
    private volatile long lastConsumeTimestamp = System.currentTimeMillis();
    /**
     * 锁
     */
    private volatile boolean locked = false;
    /**
     * 上次加锁时间
     */
    private volatile long lastLockTimestamp = System.currentTimeMillis();
    /**
     * 是否正在消费
     */
    private volatile boolean consuming = false;
    /**
     * broker端还有多少条消息没被处理（拉取消息的那一刻）
     */
    private volatile long msgAccCnt = 0;

    /**
     * 该方法是在处理消息时, 判断拥有的锁是否过期, 默认过期时间30s。
     * <p>方法在顺序消费时使用, 用来判断向broker请求消息时, 对ProcessQueue锁定时间是否超过阈值(默认30000ms, 可以通过rocketmq.client.rebalance.lockMaxLiveTime设置), 如
     * 果没有超时, 代表还是持有锁, 超过这个时间则失效。
     * @return
     */
    public boolean isLockExpired() {
        return (System.currentTimeMillis() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
    }

    /**
     * 判断拉取消息是否过期, 默认两分钟（可以通过rocketmq.client.pull.pullMaxIdleTime设置）
     * @return
     */
    public boolean isPullExpired() {
        return (System.currentTimeMillis() - this.lastPullTimestamp) > PULL_MAX_IDLE_TIME;
    }

    /**
     * 最多16条消息一处理；
     * <p>消息在客户端存在超过15分钟就被认为已过期, 然后从本地缓存中移除, 以10s的延时消息方式发送会Broker。
     * <p>顺序消费不清理过期消息, 进并发消费模式才清理过期消息。
     * @param pushConsumer
     */
    public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer) {
        // 顺序消费不清理过期消息
        if (pushConsumer.getDefaultMQPushConsumerImpl().isConsumeOrderly()) {
            return;
        }

        // 每次最多清理16条消息
        int loop = msgTreeMap.size() < 16 ? msgTreeMap.size() : 16;
        for (int i = 0; i < loop; i++) {
            MessageExt msg = null;
            try {
                // 处理消息之前, 先拿到读锁
                this.treeMapLock.readLock().lockInterruptibly();
                try {
                    if (!msgTreeMap.isEmpty()) {
                        String consumeStartTimeStamp = MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.firstEntry().getValue());
                        // 临时存放消息的treeMap不为空, 并且 判断当前时间 - TreeMap里第一条消息的开始消费时间 > 15分钟
                        if (StringUtils.isNotEmpty(consumeStartTimeStamp) && System.currentTimeMillis() - Long.parseLong(consumeStartTimeStamp) > pushConsumer.getConsumeTimeout() * 60 * 1000) {
                            msg = msgTreeMap.firstEntry().getValue();
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                } finally {
                    this.treeMapLock.readLock().unlock();
                }
            } catch (InterruptedException e) {
                log.error("getExpiredMsg exception", e);
            }

            try {
                // 把过期消息以延时消息方式重新发给 broker, 10s之后才能消费。
                pushConsumer.sendMessageBack(msg, 3);
                log.info("send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());
                try {
                    // 获取写锁
                    this.treeMapLock.writeLock().lockInterruptibly();
                    try {
                        if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey()) {
                            try {
                                // 将过期消息从本地缓存中的消息列表中移除掉
                                removeMessage(Collections.singletonList(msg));
                            } catch (Exception e) {
                                log.error("send expired msg exception", e);
                            }
                        }
                    } finally {
                        this.treeMapLock.writeLock().unlock();
                    }
                } catch (InterruptedException e) {
                    log.error("getExpiredMsg exception", e);
                }
            } catch (Exception e) {
                log.error("send expired msg exception", e);
            }
        }
    }


    /**
     * 将消息保存到 msgTreeMap 中
     * @param msgs
     * @return
     */
    public boolean putMessage(final List<MessageExt> msgs) {
        // 这个只有在顺序消费的时候才会遇到，并发消费不会用到
        boolean dispatchToConsume = false;
        try {
            // 对 map 加锁防止数据读取
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                //有效消息数量
                int validMsgCnt = 0;
                for (MessageExt msg : msgs) {
                    // 获取该位置的旧数据
                    MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);

                    // 正常来说该位点不存在消息
                    if (null == old) {
                        // 不存在消息时，有效消息数量增加
                        validMsgCnt++;
                        // 将最后一个消息的offset赋值给queueOffsetMax
                        this.queueOffsetMax = msg.getQueueOffset();
                        // 把当前消息的长度加到msgSize中
                        msgSize.addAndGet(msg.getBody().length);
                    }
                }
                // 增加有效消息总数
                msgCount.addAndGet(validMsgCnt);

                // msgTreeMap不为空(含有消息)，并且不是正在消费状态
                // // 这个值在放消息的时候会设置为true，在顺序消费模式，取不到消息则设置为false
                if (!msgTreeMap.isEmpty() && !this.consuming) {
                    // 将ProcessQueue置为正在被消费状态
                    // 有消息，且为未消费状态，则顺序消费模式可以消费
                    dispatchToConsume = true;
                    this.consuming = true;
                }

                if (!msgs.isEmpty()) {
                    // 拿到最后一条消息
                    MessageExt messageExt = msgs.get(msgs.size() - 1);
                    // 获取broker端（拉取消息时）queue里最大的offset，maxOffset会存在每条消息里
                    String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
                    // 计算broker端还有多少条消息没有被消费
                    if (property != null) {
                        // broker端的最大偏移量 - 当前ProcessQueue中处理的最大消息偏移量
                        long accTotal = Long.parseLong(property) - messageExt.getQueueOffset();
                        if (accTotal > 0) {
                            this.msgAccCnt = accTotal;
                        }
                    }
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }

        return dispatchToConsume;
    }

    /**
     * 返回processQueue中处理的一批消息中最大offset和最小offset之间的差距。
     * @return
     */
    public long getMaxSpan() {
        try {
            this.treeMapLock.readLock().lockInterruptibly();
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    // msgTreeMap中最后一个消息的offset - 第一个消息的offset
                    return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                }
            } finally {
                this.treeMapLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getMaxSpan exception", e);
        }

        return 0;
    }

    /**
     * 该方法将从ProcessQueue中移除部分消息，并行消费模式中使用。
     * @param msgs
     * @return
     */
    public long removeMessage(final List<MessageExt> msgs) {
        // 当前消费进度的下一个offset
        long result = -1;
        final long now = System.currentTimeMillis();
        try {
            // 获取写锁
            this.treeMapLock.writeLock().lockInterruptibly();
            // 最后消费的时间
            this.lastConsumeTimestamp = now;
            try {
                if (!msgTreeMap.isEmpty()) {
                    result = this.queueOffsetMax + 1;
                    // 删除的消息个数
                    int removedCnt = 0;
                    // 遍历消息，将其从Treemap中移除
                    for (MessageExt msg : msgs) {
                        MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
                        // 不为null，说明删除成功
                        if (prev != null) {
                            // 已经移除的消息数量
                            removedCnt--;
                            // ProcessQueue中消息的长度 - 当前消息长度
                            msgSize.addAndGet(0 - msg.getBody().length);
                        }
                    }
                    // ProcessQueue中的消息数量 - 删除的消息数量，即加上removedCnt（是一个负数）
                    msgCount.addAndGet(removedCnt);

                    if (!msgTreeMap.isEmpty()) {
                        // 如果还有消息存在，则使用msgTreeMap中的第一消息的offset（即最小offset）作为消费进度
                        result = msgTreeMap.firstKey();
                    }
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (Throwable t) {
            log.error("removeMessage exception", t);
        }

        return result;
    }

    public TreeMap<Long, MessageExt> getMsgTreeMap() {
        return msgTreeMap;
    }

    public AtomicLong getMsgCount() {
        return msgCount;
    }

    public AtomicLong getMsgSize() {
        return msgSize;
    }

    public boolean isDropped() {
        return dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    /**
     * 该方法把临时TreeMap中的消息全部放到msgTreeMap中，等待下一次消费。
     */
    public void rollback() {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                // 把临时TreeMap中的消息全部放到msgTreeMap中，等待下一次消费
                this.msgTreeMap.putAll(this.consumingMsgOrderlyTreeMap);
                this.consumingMsgOrderlyTreeMap.clear();
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    /**
     * 在顺序消费模式下，调用takeMessages从msgTreeMap中获取消息：其内部会将消息都放在一个临时的TreeMap中，然后进行消费。消费完消
     * 息之后，我们需要调用commit()方法将这个临时的TreeMap清除。
     * @return
     */
    public long commit() {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                // 获取临时TreeMap中最后一个消息的offset
                Long offset = this.consumingMsgOrderlyTreeMap.lastKey();
                // 消费完成之后，减去该批次的消息数量
                msgCount.addAndGet(0 - this.consumingMsgOrderlyTreeMap.size());
                // 维护ProcessQueue中的总消息长度
                for (MessageExt msg : this.consumingMsgOrderlyTreeMap.values()) {
                    // 减去每条已消费消息的长度
                    msgSize.addAndGet(0 - msg.getBody().length);
                }
                // 清除临时TreeMap中的所有消息
                this.consumingMsgOrderlyTreeMap.clear();
                // 返回下一个消费进度
                if (offset != null) {
                    return offset + 1;
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("commit exception", e);
        }

        return -1;
    }

    /**
     * 顺序消费模式下，使消息可以被重新消费。
     * @param msgs
     */
    public void makeMessageToConsumeAgain(List<MessageExt> msgs) {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                for (MessageExt msg : msgs) {
                    // 从临时TreeMap中取出消息，
                    this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
                    //再将消息放到msgTreeMap中
                    this.msgTreeMap.put(msg.getQueueOffset(), msg);
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("makeMessageToCosumeAgain exception", e);
        }
    }

    /**
     * 该方法在顺序消费模式下使用，取到消息后，就会调用我们定义的MessageListener进行消费。
     * 调用takeMessages从msgTreeMap中获取消息后：其内部会将消息都放在一个临时的TreeMap中，然后进行顺序消费。
     *
     * {@link ConsumeMessageOrderlyService.ConsumeRequest#run()}
     * @param batchSize
     * @return
     */
    public List<MessageExt> takeMessages(final int batchSize) {
        List<MessageExt> result = new ArrayList<MessageExt>(batchSize);
        final long now = System.currentTimeMillis();
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    // 从 msgTreeMap中获取batchSize条数据，每次都返回offset最小的那条消息并从msgTreeMap中移除
                    for (int i = 0; i < batchSize; i++) {
                        Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry();
                        if (entry != null) {
                            // 把消息放到返回列表
                            result.add(entry.getValue());
                            // 把消息的offset和消息体msg，放到顺序消费TreeMap中
                            consumingMsgOrderlyTreeMap.put(entry.getKey(), entry.getValue());
                        } else {
                            break;
                        }
                    }
                }

                // 没有取到消息，说明不需要消费，即将consuming置为FALSE。
                if (result.isEmpty()) {
                    consuming = false;
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("take Messages exception", e);
        }

        return result;
    }

    public boolean hasTempMessage() {
        try {
            this.treeMapLock.readLock().lockInterruptibly();
            try {
                return !this.msgTreeMap.isEmpty();
            } finally {
                this.treeMapLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
        }

        return true;
    }

    public void clear() {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.clear();
                this.consumingMsgOrderlyTreeMap.clear();
                this.msgCount.set(0);
                this.msgSize.set(0);
                this.queueOffsetMax = 0L;
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    public long getLastLockTimestamp() {
        return lastLockTimestamp;
    }

    public void setLastLockTimestamp(long lastLockTimestamp) {
        this.lastLockTimestamp = lastLockTimestamp;
    }

    public Lock getConsumeLock() {
        return consumeLock;
    }

    public long getLastPullTimestamp() {
        return lastPullTimestamp;
    }

    public void setLastPullTimestamp(long lastPullTimestamp) {
        this.lastPullTimestamp = lastPullTimestamp;
    }

    public long getMsgAccCnt() {
        return msgAccCnt;
    }

    public void setMsgAccCnt(long msgAccCnt) {
        this.msgAccCnt = msgAccCnt;
    }

    public long getTryUnlockTimes() {
        return this.tryUnlockTimes.get();
    }

    public void incTryUnlockTimes() {
        this.tryUnlockTimes.incrementAndGet();
    }

    public void fillProcessQueueInfo(final ProcessQueueInfo info) {
        try {
            this.treeMapLock.readLock().lockInterruptibly();

            if (!this.msgTreeMap.isEmpty()) {
                info.setCachedMsgMinOffset(this.msgTreeMap.firstKey());
                info.setCachedMsgMaxOffset(this.msgTreeMap.lastKey());
                info.setCachedMsgCount(this.msgTreeMap.size());
                info.setCachedMsgSizeInMiB((int) (this.msgSize.get() / (1024 * 1024)));
            }

            if (!this.consumingMsgOrderlyTreeMap.isEmpty()) {
                info.setTransactionMsgMinOffset(this.consumingMsgOrderlyTreeMap.firstKey());
                info.setTransactionMsgMaxOffset(this.consumingMsgOrderlyTreeMap.lastKey());
                info.setTransactionMsgCount(this.consumingMsgOrderlyTreeMap.size());
            }

            info.setLocked(this.locked);
            info.setTryUnlockTimes(this.tryUnlockTimes.get());
            info.setLastLockTimestamp(this.lastLockTimestamp);

            info.setDroped(this.dropped);
            info.setLastPullTimestamp(this.lastPullTimestamp);
            info.setLastConsumeTimestamp(this.lastConsumeTimestamp);
        } catch (Exception e) {
        } finally {
            this.treeMapLock.readLock().unlock();
        }
    }

    public long getLastConsumeTimestamp() {
        return lastConsumeTimestamp;
    }

    public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
        this.lastConsumeTimestamp = lastConsumeTimestamp;
    }

}
