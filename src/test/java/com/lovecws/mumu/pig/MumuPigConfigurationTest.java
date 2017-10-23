package com.lovecws.mumu.pig;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.pig.PigServer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.junit.Test;

import java.io.IOException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 配置测试
 * @date 2017-10-23 15:41
 */
public class MumuPigConfigurationTest {

    private MumuPigConfiguration pigConfiguration = new MumuPigConfiguration();

    @Test
    public void local() {
        PigServer pigServer = pigConfiguration.local();
        try {
            pigServer.printAliases();
        } catch (FrontendException e) {
            e.printStackTrace();
        }
        System.out.println(pigServer);
    }

    @Test
    public void mapreduce() {
        PigServer pigServer = pigConfiguration.mapreduce();
        System.out.println(pigServer);
    }

    @Test
    public void distributedFileSystem() {
        DistributedFileSystem distributedFileSystem = pigConfiguration.distributedFileSystem();
        try {
            FileStatus[] fileStatuses = distributedFileSystem.listStatus(new Path("/"));
            for (FileStatus fileStatus : fileStatuses) {
                System.out.println(fileStatus);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pigConfiguration.close(distributedFileSystem);
        }
    }
}
