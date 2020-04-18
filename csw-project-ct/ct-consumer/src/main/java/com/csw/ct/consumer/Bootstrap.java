package com.csw.ct.consumer;

import com.csw.ct.common.bean.Consumer;
import com.csw.ct.consumer.bean.CalllogConsumer;

/**
 * 启动消费者
 * //使用kafka消费者获取flume采集的数据
 *
 * //将数据存储到Hbase中
 */
public class Bootstrap {
    public static void main(String[] args) throws Exception{

        //创建消费者
        Consumer consumer = new CalllogConsumer ();

        //消费数据
        consumer.consume ();

        //关闭资源
        consumer.close ();
    }
}
