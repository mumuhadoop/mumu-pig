package com.lovecws.mumu.pig.query;

import com.lovecws.mumu.pig.MumuPigConfiguration;
import org.apache.pig.PigServer;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: pig查询
 * @date 2017-10-23 16:30
 */
public class PigQuery {

    /**
     * 普通语句查询
     *
     * @param query
     * @param storeId
     * @param storeFile
     */
    public void query(String query, String storeId, String storeFile) {
        PigServer pigServer = null;
        try {
            pigServer = new MumuPigConfiguration().mapreduce();
            pigServer.debugOn();
            pigServer.registerQuery(query);
            if (storeId != null) {
                pigServer.store(storeId, storeFile);
            }
        } catch (IOException e) {
            e.printStackTrace();
            pigServer.shutdown();
        }
    }

    /**
     * 运行脚本
     *
     * @param scriptPath
     * @param storeId
     * @param storeFile
     */
    public void script(InputStream scriptPath, String storeId, String storeFile) {
        PigServer pigServer = null;
        try {
            pigServer = new MumuPigConfiguration().mapreduce();
            pigServer.debugOn();
            pigServer.registerScript(scriptPath);
            if (storeId != null) {
                pigServer.store(storeId, storeFile);
            }
        } catch (IOException e) {
            e.printStackTrace();
            pigServer.shutdown();
        }
    }

    public void jar() {
        PigServer pigServer = null;
        try {
            pigServer = new MumuPigConfiguration().mapreduce();
            pigServer.registerJar("mumu-pig.jar");
            pigServer.registerQuery("nginxlog = load '/mapreduce/nginxlog/access/input' using com.lovecws.mumu.pig.loader.NginxLogLoader();");
            pigServer.dumpSchema("nginxlog");
        } catch (IOException e) {
            e.printStackTrace();
            pigServer.shutdown();
        }
    }
}
