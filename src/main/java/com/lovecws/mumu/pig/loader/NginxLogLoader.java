package com.lovecws.mumu.pig.loader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: nginx日志加载
 * @date 2017-10-20 16:51
 */
public class NginxLogLoader extends LoadFunc {

    protected RecordReader recordReader = null;

    @Override
    public void setLocation(final String s, final Job job) throws IOException {
        FileInputFormat.setInputPaths(job, s);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new PigTextInputFormat();
    }

    @Override
    public void prepareToRead(final RecordReader recordReader, final PigSplit pigSplit) throws IOException {
        this.recordReader = recordReader;
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            boolean flag = recordReader.nextKeyValue();
            if (!flag) {
                return null;
            }
            Text value = (Text) recordReader.getCurrentValue();
            Map<String, Object> stringObjectMap = NginxAccessLogParser.parseLine(value.toString());
            List propertiyList = new ArrayList<String>();
            int i = 0;
            for (Object object : stringObjectMap.values()) {
                propertiyList.add(i++, object.toString());
            }
            return TupleFactory.getInstance().newTuple(propertiyList);
        } catch (InterruptedException e) {
            throw new ExecException("Read data error", PigException.REMOTE_ENVIRONMENT, e);
        }
    }
}
