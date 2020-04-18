package com.csw.ct.analysis.tool;

import com.csw.ct.analysis.io.MySQLTextOutputFormat;
import com.csw.ct.analysis.mapper.AnalysisTextMapper;
import com.csw.ct.analysis.reduce.AnalysisTextReducer;
import com.csw.ct.common.constant.Names;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

/**
 * 分析数据工具类
 */
public class AnalysisTextTool implements Tool {

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass (AnalysisTextTool.class);

        Scan scan = new Scan ();
        scan.addFamily ( Bytes.toBytes (Names.CF_CALLER.getValue ()) );
        //mapper，参数（表名，Scan，Mapper）
        TableMapReduceUtil.initTableMapperJob (
                Names.TABLE.getValue(),
                scan,
                AnalysisTextMapper.class,
                Text.class,
                Text.class,
                job
                );

        //reducer
        job.setReducerClass ( AnalysisTextReducer.class );
        job.setOutputKeyClass ( Text.class );
        job.setOutputValueClass ( Text.class );
        //outputformat
        job.setOutputFormatClass ( MySQLTextOutputFormat.class );

        boolean flg = job.waitForCompletion ( true );
        if ( flg ) {
            return JobStatus.State.SUCCEEDED.getValue ();
        } else {
            return JobStatus.State.FAILED.getValue ();
        }
    }

    public void setConf(Configuration configuration) {

    }

    public Configuration getConf() {
        return null;
    }
}
