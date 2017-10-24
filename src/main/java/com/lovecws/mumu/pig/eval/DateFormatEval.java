package com.lovecws.mumu.pig.eval;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 日期转换
 * @date 2017-10-24 9:36
 */
public class DateFormatEval extends EvalFunc<String> {

    private String formatPattern;

    public DateFormatEval() {
        this.formatPattern = "yyyy-MM-dd";
    }

    public DateFormatEval(final String formatPattern) {
        this.formatPattern = formatPattern;
    }

    @Override
    public String exec(final Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() == 0) {
            return null;
        }
        Object dateString = tuple.get(0);
        if (dateString == null) {
            return null;
        }
        try {
            Date date = DateUtils.parseDate(dateString.toString(), new String[]{"yyyy-MM-dd HH:mm:ss"});
            return DateFormatUtils.format(date, formatPattern);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

}
