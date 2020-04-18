package com.csw.ct.producer;

import com.csw.ct.common.bean.Producer;
import com.csw.ct.producer.bean.LocalFileProducer;
import com.csw.ct.producer.io.LocalFileDatain;
import com.csw.ct.producer.io.LocalFileDataout;

/**
 * 启动对象
 */
public class Bootstrap {
    public static void main(String[] args) throws Exception {

        //构建生产者对象
        Producer producer = new LocalFileProducer();

        if(args.length < 2) {
            System.out.println("no arguments");
            System.exit(1);
        }
        //执行命令  java -jar Produce.jar path1 path2
        producer.setIn(new LocalFileDatain(args[0]));
        producer.setOut(new LocalFileDataout(args[1]));
        //producer.setIn(new LocalFileDatain("D:\\大数据学习资料\\项目_电信客服\\尚硅谷大数据技术之电信客服综合案例\\2.资料\\辅助文档\\contact.log"));
        //producer.setOut(new LocalFileDataout("D:\\大数据学习资料\\项目_电信客服\\尚硅谷大数据技术之电信客服综合案例\\2.资料\\辅助文档\\call.log"));

        //生产数据
        producer.produce();

        //关闭生产者对象
        producer.close();
    }
}
