package com.csw.ct.common.bean;

import com.csw.ct.common.api.Column;
import com.csw.ct.common.api.Rowkey;
import com.csw.ct.common.api.TableRef;
import com.csw.ct.common.constant.Names;
import com.csw.ct.common.constant.ValueConstant;
import com.csw.ct.common.util.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

/**
 * 基础数据访问对象
 */
public abstract class BaseDao {

    private ThreadLocal<Connection> connHolder = new ThreadLocal<Connection> ();
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<Admin> ();

    protected void start() throws Exception {
        getConnection ();
        getAdmin ();
    }

    protected void end() throws Exception {
        Admin admin = getAdmin ();
        if( admin != null ) {
            admin.close ();
            adminHolder.remove ();
        }
        Connection conn = getConnection ();
        if( conn != null ) {
            conn.close ();
            connHolder.remove ();
        }
    }

    /**
     * 创建表，如果表已存在，则删除，删除后再创建新表
     * @param name
     * @param families
     */
    protected void createTableXX(String name, String... families) throws Exception{
        createTableXX ( name,null,families );
    }

    protected void createTableXX(String name, Integer regionCount, String... families) throws Exception{
        Admin admin = getAdmin ();
        TableName tableName = TableName.valueOf ( name );

        if(admin.tableExists (tableName)) {
            //表存在，删除表
            deleteTable ( name );
        }
        //创建表
        createTable(name, regionCount, families);
    }

    private void createTable(String name, Integer regionCount, String... families) throws Exception {
        Admin admin = getAdmin ();
        TableName tableName = TableName.valueOf ( name );
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        if(families == null || families.length == 0) {
            families = new String[1];
            families[0] = Names.CF_INFO.getValue ();
        }
        for (String family : families) {
            HColumnDescriptor columnDescriptor =
                    new HColumnDescriptor (family);
            tableDescriptor.addFamily (columnDescriptor);
        }

        //增加预分区
        if ( regionCount == null || regionCount <= 0) {
            admin.createTable (tableDescriptor);
        } else {
            //分区键
            byte[][] splitKeys = genSplitKeys(regionCount);
            admin.createTable (tableDescriptor, splitKeys);
        }
    }

    /**
     * 获取查询时startrow,stoprow集合
     * @return
     */
    protected List<String[]> getStartStopRowkeys(String tel, String start, String end) {
        List<String[]> rowkeyss = new ArrayList<String[]>();

        String startTime = start.substring ( 0,6 );
        String endTime = end.substring ( 0,6 );
        Calendar startCal = Calendar.getInstance ();
        startCal.setTime ( DateUtil.parse (startTime, "yyyyMM"));
        Calendar endCal = Calendar.getInstance ();
        endCal.setTime ( DateUtil.parse (endTime, "yyyyMM"));

        while(startCal.getTimeInMillis () <= endCal.getTimeInMillis ()){
            String nowTime = DateUtil.format ( startCal.getTime (), "yyyyMM" );

            int regionNum = genRegionNum ( tel, nowTime );

            String startRow = regionNum + "_" + tel + "_" + nowTime;
            String stopRow = startRow + "|";

            String[] rowkeys = {startRow, stopRow};
            rowkeyss.add(rowkeys);

            startCal.add(Calendar.MONTH,1);
        }

        return rowkeyss;
    }
    /**
     * 计算生成分区号
     * @return
     */
    protected int genRegionNum(String tel, String date) {
        //tel电话号码
        String usercode = tel.substring ( tel.length () - 4 );
        //20181010120000 取年月201810
        String yearMonth = date.substring ( 0,6 );
        int userCodeHash = usercode.hashCode ();
        int yearMonthHash = yearMonth.hashCode ();

        //crc异或校验
        int crc = Math.abs(userCodeHash ^ yearMonthHash);

        int regionNum = crc % ValueConstant.REGION_COUNT;
        return regionNum;
    }
    /**
     * 生成分区键
     * @return
     */
    private byte[][] genSplitKeys(int regionCount) {
        int splitkeyCount = regionCount - 1;
        byte[][] bs = new byte[splitkeyCount][];
        List<byte[]> bslist = new ArrayList<byte[]> ();
        for (int i = 0; i < splitkeyCount; i++) {
            String splitkey = i + "|";
            bslist.add( Bytes.toBytes (splitkey));
        }
        bslist.toArray (bs);
        return bs;
    }

    protected void putData(Object obj) throws Exception{
        //反射
        Class clazz = obj.getClass ();
        TableRef tableRef = (TableRef) clazz.getAnnotation ( TableRef.class );
        String tableName = tableRef.value ();

        Field[] fs = clazz.getDeclaredFields ();
        String stringRowkey = "";
        for (Field f : fs) {
            Rowkey rowkey = f.getAnnotation ( Rowkey.class );
            if ( rowkey != null) {
                f.setAccessible ( true );
                stringRowkey = (String) f.get(obj);
                break;
            }
        }

        //获取表对象
        Connection conn = getConnection ();
        Table table = conn.getTable ( TableName.valueOf ( tableName ) );
        Put put = new Put(Bytes.toBytes ( stringRowkey ));
        for (Field f : fs) {
            Column column = f.getAnnotation ( Column.class );
            if ( column != null) {
                String family = column.family ();
                String colName = column.column();
                if( colName == null || "".equals (colName)) {
                    colName = f.getName ();
                }
                f.setAccessible ( true );
                String value = (String) f.get(obj);
                put.addColumn ( Bytes.toBytes ( family ), Bytes.toBytes ( colName ), Bytes.toBytes ( value ));
            }
        }
        //增加数据
        table.put(put);
        //关闭表
        table.close ();
    }

    /**
     * 插入数据
     * @param name
     * @param put
     */
    protected void putData(String name, Put put) throws Exception{
        //获取表对象
        Connection conn = getConnection ();
        Table table = conn.getTable ( TableName.valueOf ( name ) );
        //增加数据
        table.put(put);
        //关闭表
        table.close ();
    }

    protected void deleteTable(String name) throws Exception {
        Admin admin = getAdmin ();
        TableName tableName = TableName.valueOf ( name );
        admin.disableTable ( tableName );
        admin.deleteTable ( tableName );
    }

    /**
     * 创建命名空间，若已存在，则不需要创建
     * @param namespace
     */
    protected void createNamespaceNX( String namespace ) throws Exception {
        Admin admin = getAdmin ();
        try {
            admin.getNamespaceDescriptor ( namespace );
        } catch ( NamespaceNotFoundException e) {
            //e.printStackTrace ();
            NamespaceDescriptor namespaceDescriptor =
                    NamespaceDescriptor.create(namespace).build();
            admin.createNamespace (namespaceDescriptor);
        }
    }

    /**
     * 获取管理对象
     * @return
     */
    protected synchronized Admin getAdmin() throws Exception{
        Admin admin = adminHolder.get();
        if( admin == null ) {
            admin = getConnection ().getAdmin ();
            adminHolder.set ( admin );
        }
        return admin;
    }

    /**
     * 获取连接对象
     * @return
     */
    protected synchronized Connection getConnection() throws Exception{
        Connection conn = connHolder.get();
        if( conn == null ) {
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);

            connHolder.set(conn);
        }
        return conn;
    }
}
