package com.lovecws.mumu.pig.query;

import com.lovecws.mumu.pig.MumuPigConfiguration;
import org.apache.commons.lang.time.DateFormatUtils;
import org.junit.Test;

import java.util.Date;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 查询测试
 * @date 2017-10-23 16:31
 */
public class PigQueryTest {

    private PigQuery pigQuery = new PigQuery();

    @Test
    public void passwd() {
        StringBuilder builder = new StringBuilder();
        builder.append("password = load '/pig/passwd' using PigStorage(':');");
        builder.append("username_password = foreach password generate $0 as username;");
        pigQuery.query(builder.toString(), "username_password", "output/password" + DateFormatUtils.format(new Date(), "yyyyMMddHHmmss"));
    }

    @Test
    public void passwdScript() {
        String filename = "/pig/output/password" + DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
        pigQuery.script(PigQueryTest.class.getResourceAsStream("/pig/password.pig"), "username_password", filename);
        new MumuPigConfiguration().printMessage(filename);
    }

    @Test
    public void maxTemperatureScript() {
        String filename = "/pig/output/maxTemperature" + DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
        pigQuery.script(PigQueryTest.class.getResourceAsStream("/pig/maxTemperature.pig"), "max_temperature_records", filename);
        new MumuPigConfiguration().printMessage(filename);
    }

    @Test
    public void nginxLogScript() {
        String filename = "/pig/output/nginxlog" + DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
        pigQuery.script(PigQueryTest.class.getResourceAsStream("/pig/nginxlog_ipcounter.pig"), "limit_nginxlog", filename);
        new MumuPigConfiguration().printMessage(filename);
    }

    @Test
    public void hourNginxLogScript() {
        String filename = "hdfs://192.168.11.25:9000/pig/output/nginxlog" + DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
        pigQuery.script(PigQueryTest.class.getResourceAsStream("/pig/nginxlog_timecounter.pig"), "counter_nginxlog", filename);
        new MumuPigConfiguration().printMessage(filename);
    }

    @Test
    public void jar(){
        pigQuery.jar();
    }
}
