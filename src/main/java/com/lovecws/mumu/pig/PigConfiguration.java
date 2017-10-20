package com.lovecws.mumu.pig;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

import java.io.IOException;
import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-10-20 14:54
 */
public class PigConfiguration {
    public static void main(String[] args) throws IOException {
        PigServer pigServer = new PigServer(ExecType.LOCAL);
        pigServer.debugOn();
        Map<String, LogicalPlan> aliases = pigServer.getAliases();
        System.out.println(aliases);
    }
}
