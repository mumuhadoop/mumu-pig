package com.lovecws.mumu.pig.loader;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.Expression;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;

import java.io.IOException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: nginx日志加载
 * @date 2017-10-20 16:51
 */
public class NginxLogLoader implements LoadMetadata {
    @Override
    public ResourceSchema getSchema(final String s, final Job job) throws IOException {
        return null;
    }

    @Override
    public ResourceStatistics getStatistics(final String s, final Job job) throws IOException {
        return null;
    }

    @Override
    public String[] getPartitionKeys(final String s, final Job job) throws IOException {
        return new String[0];
    }

    @Override
    public void setPartitionFilter(final Expression expression) throws IOException {

    }
}
