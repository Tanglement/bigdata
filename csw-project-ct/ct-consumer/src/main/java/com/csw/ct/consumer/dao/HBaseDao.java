package com.csw.ct.consumer.dao;

import com.csw.ct.common.bean.BaseDao;
import com.csw.ct.common.constant.Names;
import com.csw.ct.common.constant.ValueConstant;
import com.csw.ct.consumer.bean.Calllog;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase数据访问对象
 */
public class HBaseDao extends BaseDao {
    /**
     * 初始化
     */
    public void init() throws Exception{
        start ();

        //创建命名空间和表
        createNamespaceNX( Names.NAMESPACE.getValue ());
        createTableXX(Names.TABLE.getValue (), ValueConstant.REGION_COUNT, Names.CF_CALLER.getValue ());

        end ();
    }

    /**
     * 插入对象
     * @param log
     * @throws Exception
     */
    public void insertDate(Calllog log) throws Exception {
        log.setRowkey (genRegionNum (log.getCall1 (), log.getCalltime ()) + "_"+ log.getCall1 () + "_" + log.getCalltime () + "_" + log.getCall2 () + "_" + log.getDuration ());
        putData ( log );
    }

    /**
     * 插入数据
     * @param value
     */
    public void insertDate(String value) throws Exception{
        //将通话日志保存到HBase表中

        //获取通话日志
        String[] values = value.split ( "\t" );
        String call1 = values[0];
        String call2 = values[1];
        String calltime = values[2];
        String duration = values[3];

        //创建数据对象
        //rowkey设计,长度,唯一性,散列
        //rowkey = regionNum + call1 + calltime + call2 + duration
        String rowkey = genRegionNum (call1, calltime) + "_"+ call1 + "_" + calltime + "_" +call2 + "_" + duration;

        byte[] family = Bytes.toBytes ( Names.CF_CALLER.getValue () );

        Put put = new Put( Bytes.toBytes (rowkey));
        put.addColumn (family, Bytes.toBytes ( "call1" ) ,Bytes.toBytes ( call1 ) );
        put.addColumn (family, Bytes.toBytes ( "call2" ) ,Bytes.toBytes ( call2 ) );
        put.addColumn (family, Bytes.toBytes ( "calltime" ) ,Bytes.toBytes ( calltime ) );
        put.addColumn (family, Bytes.toBytes ( "duration" ) ,Bytes.toBytes ( duration ) );

        //保存数据
        putData(Names.TABLE.getValue (), put);
    }
}
