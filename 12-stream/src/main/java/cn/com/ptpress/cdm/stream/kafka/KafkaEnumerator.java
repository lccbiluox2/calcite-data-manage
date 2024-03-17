package cn.com.ptpress.cdm.stream.kafka;

import com.alibaba.fastjson.JSONObject;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.util.Source;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

public class KafkaEnumerator<E> implements Enumerator<E> {

    private E current;

    private KafkaConsumerThread thread;
    private ArrayBlockingQueue<JSONObject> blockingQueue = new ArrayBlockingQueue<>(100);


    KafkaEnumerator(Properties properties) {
        thread = new KafkaConsumerThread(properties, (ArrayBlockingQueue<com.alibaba.fastjson.JSONObject>) blockingQueue);
        thread.setDaemon(true);
        thread.setName("kafka-consumer");
        thread.start();
    }

    @Override
    public E current() {
        return current;
    }

    /**
     * 判断是否有下一行，并更新current
     *
     * @return
     */
    @Override
    public boolean moveNext() {
        try {
            System.out.println("日志> 准备获取下一条数据..");
            if (blockingQueue.isEmpty()) {
                // 为空的时候 不能直接返回false，否则程序会直接结束
                // 这里如果你返回了 true,那么他会立即调用current() 方法获取数据
                // 然后会导致结果为空，因此这里我们改成死等数据
                JSONObject take = blockingQueue.take();
                current = toArrayType(take);
                return true;
            }
            JSONObject take = blockingQueue.take();
            current = toArrayType(take);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private E toArrayType(JSONObject take) {
        Object[] objects = new Object[3];

        String log_time = take.getString("LOG_TIME");
        String LEVEL = take.getString("LEVEL");
        String MSG = take.getString("MSG");

        objects[0] = log_time;
        objects[1] = LEVEL;
        objects[2] = MSG;

        return (E) objects;
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException("Error");
    }

    @Override
    public void close() {
        try {
            thread.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
