package com.csw.ct.cache;

import com.csw.ct.common.util.JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 启动缓存客户端，向redis中增加缓存数据
 */
public class Bootstrap {
    public static void main(String[] args) {
        Connection connection = null;
        //读取mysql数据
        Map<String, Integer> userMap = new HashMap<String, Integer> ();
        Map<String, Integer> dateMap = new HashMap<String, Integer>();
        //读取用户、时间数据
        PreparedStatement pstat = null;
        ResultSet rs = null;
        try {
            connection = JDBCUtil.getConnection ();
            String queryUserSql = "select id, tel from ct_user";
            pstat = connection.prepareStatement (queryUserSql);
            rs = pstat.executeQuery ();
            while(rs.next ()) {
                Integer id = rs.getInt ( 1 );
                String tel = rs.getString ( 2 );
                userMap.put ( tel, id );
            }
            rs.close();

            String queryDateSql = "select id,year,month,day from ct_date";
            pstat = connection.prepareStatement ( queryDateSql );
            rs = pstat.executeQuery ();
            while(rs.next ()) {
                Integer id = rs.getInt ( 1 );
                String year = rs.getString ( 2 );
                String month = rs.getString ( 3 );
                String day = rs.getString ( 4 );
                dateMap.put ( year+month+day,id );
            }
        } catch (SQLException e) {
            e.printStackTrace ();
        } finally {
            if(rs != null) {
                try {
                    rs.close ();
                } catch (SQLException e) {
                    e.printStackTrace ();
                }
            }
            if( pstat != null) {
                try {
                    pstat.close ();
                } catch (SQLException e) {
                    e.printStackTrace ();
                }
            }
            if( connection != null) {
                try {
                    connection.close ();
                } catch (SQLException e) {
                    e.printStackTrace ();
                }
            }
        }
        //向redis存储数据,主机名和端口号
        Jedis jedis = new Jedis ("master", 6379);
        Iterator<String> keyIterator = userMap.keySet ().iterator ();
        while( keyIterator.hasNext ()) {
            String key = keyIterator.next ();
            Integer value = userMap.get ( key );
            jedis.hset("ct_user", key, ""+value);
        }

        keyIterator = dateMap.keySet ().iterator ();
        while( keyIterator.hasNext ()) {
            String key = keyIterator.next ();
            Integer value = dateMap.get ( key );
            jedis.hset("ct_date", key, ""+value);
        }

    }
}
