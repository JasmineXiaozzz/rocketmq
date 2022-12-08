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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely extends AbstractAllocateMessageQueueStrategy {

    /**
     *
     * @param consumerGroup current consumer group 消费者组
     * @param currentCID current consumer id 客户端ID
     * @param mqAll message queue set in current topic 所有队列
     * @param cidAll consumer set in current consumer group 所有客户端ID
     * @return
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return result;
        }

        // 当前客户端在所有客户端的下标
        int index = cidAll.indexOf(currentCID);
        // 队列数 >= 客户端数：0
        // 队列数 <  客户端数：4
        int mod = mqAll.size() % cidAll.size();
        int averageSize =
            /**
             * mq 的个数 <= 客户端的个数, 平均长度为1
             * mq 的个数 > 客户端的个数, 平均个数为：
             * - 队列数 < 客户端数 && 客户端下标小于队列数, 则平均长度为
             */
            mqAll.size() <= cidAll.size() ?
                1 :
                (mod > 0 && index < mod ?
                    mqAll.size() / cidAll.size() + 1 :
                    mqAll.size() / cidAll.size()
                );
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
