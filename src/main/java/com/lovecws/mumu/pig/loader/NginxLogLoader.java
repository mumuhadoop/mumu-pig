package com.lovecws.mumu.pig.loader;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.*;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: nginx日志加载
 * @date 2017-10-20 16:51
 */
public class NginxLogLoader extends LoadFunc implements LoadMetadata {

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
            propertiyList.add(0, stringObjectMap.get("remoteAddr"));
            propertiyList.add(1, DateFormatUtils.format((Date) stringObjectMap.get("accessTime"), "yyyy-MM-dd HH:mm:ss"));
            propertiyList.add(2, stringObjectMap.get("method"));

            propertiyList.add(3, stringObjectMap.get("url"));
            propertiyList.add(4, stringObjectMap.get("protocol"));
            propertiyList.add(5, stringObjectMap.get("agent"));

            propertiyList.add(6, stringObjectMap.get("refer"));
            propertiyList.add(7, stringObjectMap.get("status"));
            propertiyList.add(8, stringObjectMap.get("length"));
            return TupleFactory.getInstance().newTuple(propertiyList);
        } catch (InterruptedException e) {
            throw new ExecException("Read data error", PigException.REMOTE_ENVIRONMENT, e);
        }
    }

    @Override
    public ResourceSchema getSchema(final String s, final Job job) throws IOException {
        ResourceSchema.ResourceFieldSchema remoteAddrFieldSchema = new ResourceSchema.ResourceFieldSchema(new Schema.FieldSchema("remoteAddr", DataType.BYTEARRAY));
        ResourceSchema.ResourceFieldSchema accessTimeFieldSchema = new ResourceSchema.ResourceFieldSchema(new Schema.FieldSchema("accessTime", DataType.BYTEARRAY));
        ResourceSchema.ResourceFieldSchema methodFieldSchema = new ResourceSchema.ResourceFieldSchema(new Schema.FieldSchema("method", DataType.BYTEARRAY));

        ResourceSchema.ResourceFieldSchema urlFieldSchema = new ResourceSchema.ResourceFieldSchema(new Schema.FieldSchema("url", DataType.BYTEARRAY));
        ResourceSchema.ResourceFieldSchema protocolFieldSchema = new ResourceSchema.ResourceFieldSchema(new Schema.FieldSchema("protocol", DataType.BYTEARRAY));
        ResourceSchema.ResourceFieldSchema agentFieldSchema = new ResourceSchema.ResourceFieldSchema(new Schema.FieldSchema("agent", DataType.BYTEARRAY));

        ResourceSchema.ResourceFieldSchema referFieldSchema = new ResourceSchema.ResourceFieldSchema(new Schema.FieldSchema("refer", DataType.BYTEARRAY));
        ResourceSchema.ResourceFieldSchema statusFieldSchema = new ResourceSchema.ResourceFieldSchema(new Schema.FieldSchema("status", DataType.INTEGER));
        ResourceSchema.ResourceFieldSchema lengthFieldSchema = new ResourceSchema.ResourceFieldSchema(new Schema.FieldSchema("length", DataType.INTEGER));

        ResourceSchema resourceSchema = new ResourceSchema();
        resourceSchema.setFields(new ResourceSchema.ResourceFieldSchema[]{remoteAddrFieldSchema, accessTimeFieldSchema, methodFieldSchema, urlFieldSchema, protocolFieldSchema, agentFieldSchema, referFieldSchema, statusFieldSchema, lengthFieldSchema});
        return resourceSchema;
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
