package com.csw.ct.producer.bean;

import com.csw.ct.common.bean.Datain;
import com.csw.ct.common.bean.Dataout;
import com.csw.ct.common.bean.Producer;
import com.csw.ct.common.util.DateUtil;
import com.csw.ct.common.util.NumberUtil;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * 本地数据文件生产者
 */
public class LocalFileProducer implements Producer {

    private Datain in;
    private Dataout out;
    private volatile boolean flag =true;

    public void setIn(Datain in) {
        this.in = in;
    }

    public void setOut(Dataout out) {
        this.out = out;
    }

    /**
     * 生产数据
     */
    public void produce() {

        try {
            //读取通讯录数据
            List<Contact> contacts = in.read(Contact.class);
//            for (Contact contact : contacts) {
//                System.out.println (contact);
//            }
            while(flag) {
                //从通讯录中随机查找两个电话号码（主叫、被叫）
                int call1Index = new Random().nextInt(contacts.size ());
                int call2Index;
                while ( true ) {
                    call2Index = new Random().nextInt(contacts.size ());
                    if(call1Index != call2Index) {
                        break;
                    }
                }
                Contact call1 = contacts.get(call1Index);
                Contact call2 = contacts.get(call2Index);

                //生成随机的通话时间
                String startDate = "20180101000000";
                String endDate = "20190101000000";
                long startTime = DateUtil.parse ( startDate, "yyyyMMddHHmmss").getTime ();
                long endTime = DateUtil.parse ( endDate,"yyyyMMddHHmmss" ).getTime ();

                //童话世界
                long callTime = startTime + (long)((endTime - startTime) * Math.random ());
                String callTimeString = DateUtil.format ( new Date (callTime),"yyyyMMddHHmmss" );
                //生成随机的通话时长
                String duration = NumberUtil.format(new Random ().nextInt (3000), 4);

                //生成通话记录
                Calllog log = new Calllog (call1.getTel (),call2.getTel (),callTimeString,duration);
                //将通话记录刷写到数据文件中
                System.out.println (log);
                out.write ( log );
                Thread.sleep ( 500 );
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭生成者
     * @throws IOException
     */
    public void close() throws IOException {
        if(in != null) {
            in.close();
        }

        if(out != null) {
            out.close();
        }
    }
}
