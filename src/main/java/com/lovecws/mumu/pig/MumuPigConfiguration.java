package com.lovecws.mumu.pig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.pig.ExecType;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-10-20 14:54
 */
public class MumuPigConfiguration {

    public static String HADOOP_ADDRESS = "hdfs://192.168.11.25:9000";

    /**
     * 获取到本地模式下的pigServer
     *
     * @return
     */
    public PigServer local() {
        PigServer pigServer = null;
        try {
            pigServer = new PigServer(ExecType.LOCAL);
        } catch (ExecException e) {
            e.printStackTrace();
        }
        return pigServer;
    }

    /**
     * 获取到mapreduce下的pigServer
     *
     * @return
     */
    public PigServer mapreduce() {
        PigServer pigServer = null;
        try {
            Configuration configuration = new Configuration();
            //默认需要将配置文件放置在classpath下，但是可以通过这个属性来修改配置文件路径
            configuration.set("pig.use.overriden.hadoop.configs", "true");
            //优先从环境变量中获取hadoop服务器地址
            String hadoopAddress = System.getenv("HADOOP_ADDRESS");
            if (hadoopAddress != null) {
                HADOOP_ADDRESS = hadoopAddress;
            }
            configuration.set("fs.defaultFS", HADOOP_ADDRESS);
            configuration.set(PigConfiguration.PIG_AUTO_LOCAL_ENABLED, "false");
            pigServer = new PigServer(ExecType.MAPREDUCE, configuration);
        } catch (ExecException e) {
            e.printStackTrace();
        }
        return pigServer;
    }

    public DistributedFileSystem distributedFileSystem() {
        DistributedFileSystem distributedFileSystem = new DistributedFileSystem();
        try {
            distributedFileSystem.initialize(new URI(HADOOP_ADDRESS), new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return distributedFileSystem;
    }

    public void close(DistributedFileSystem distributedFileSystem) {
        if (distributedFileSystem != null) {
            try {
                distributedFileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void printMessage(String path) {
        System.out.println("\nprint result:");
        DistributedFileSystem distributedFileSystem = distributedFileSystem();
        try {
            FileStatus[] fileStatuses = distributedFileSystem.listStatus(new Path(path));
            for (FileStatus fileStatus : fileStatuses) {
                System.out.println(fileStatus);
                if (fileStatus.isFile()) {
                    FSDataInputStream fsDataInputStream = distributedFileSystem.open(fileStatus.getPath());
                    byte[] bs = new byte[fsDataInputStream.available()];
                    fsDataInputStream.read(bs);
                    fsDataInputStream.close();
                    System.out.println(new String(bs));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(distributedFileSystem);
        }
    }
}
