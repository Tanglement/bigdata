package com.csw.ct.analysis.io;

import com.csw.ct.common.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * MySQL数据格式化输出对象
 */
public class MySQLTextOutputFormat extends OutputFormat<Text, Text> {

    protected static class MySQLRecordWrite extends RecordWriter<Text, Text> {

        private Connection connection = null;
        public MySQLRecordWrite() {
            connection = JDBCUtil.getConnection ();
        }

        @Override
        public void write(Text key, Text value) throws IOException, InterruptedException {

            String[] keys = key.toString ().split ( "_" );
            String tel = keys[0];
            String date = keys[1];

            String[] values = value.toString ().split ( "_" );
            String sumcall = values[0];
            String sumduration = values[1];

            PreparedStatement pstat = null;
            try {
                String insertSQL = "insert into ct_call(tel, date, sumcall, sumduration) values(?,?,?,?)";
                pstat = connection.prepareStatement (insertSQL);
                pstat.setString ( 1, tel );
                pstat.setString ( 2, date);
                pstat.setInt ( 3, Integer.parseInt (sumcall));
                pstat.setInt ( 4, Integer.parseInt(sumduration));
                pstat.executeUpdate ();
            } catch (SQLException e) {
                e.printStackTrace ();
            } finally {
                if( pstat != null) {
                    try {
                        pstat.close();
                    } catch (Exception e) {
                        e.printStackTrace ();
                    }
                }
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if(connection != null) {
                try {
                    connection.close ();
                } catch (SQLException e) {
                    e.printStackTrace ();
                }
            }
        }
    }

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MySQLRecordWrite ();
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    private FileOutputCommitter committer = null;
    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration ().get( FileOutputFormat.OUTDIR );
        return name == null ? null : new Path(name);
    }
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if(committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter ( output, context );
        }
        return committer;
    }
}
