package cn.com.ptpress.cdm.stream.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

public class KafkaConsumerThread extends Thread {

    private ArrayBlockingQueue<JSONObject> blockingQueue = null;
    private KafkaConsumer<String, String> consumer = null;

    public KafkaConsumerThread(Properties properties,
                               ArrayBlockingQueue<JSONObject> blockingQueue) {
        this.blockingQueue = blockingQueue;
        String topic = (String) properties.get("topic");
        this.consumer = new KafkaConsumer<>(properties);
        // 最开始的订阅列表：atopic、btopic
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(2000);//表示每2秒consumer就有机会去轮询一下订阅状态是否需要变更
            System.out.println("日志> 消费条数:" + poll.count());
            if (poll.isEmpty()) {
                continue;
            }
            Iterator<ConsumerRecord<String, String>> iterator = poll.iterator();
            while (iterator.hasNext()) {
                ConsumerRecord<String, String> next = iterator.next();
                String value = next.value();
                if (StringUtils.isBlank(value)) continue;
                try {
                    System.out.println("日志> 消费到数据:" + value);
                    JSONObject parse = JSONObject.parseObject(value);
                    blockingQueue.put(parse);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
