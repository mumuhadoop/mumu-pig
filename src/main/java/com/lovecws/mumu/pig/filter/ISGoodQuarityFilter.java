package com.lovecws.mumu.pig.filter;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 判断天气质量是否是好数据
 * @date 2017-10-20 15:07
 */
public class ISGoodQuarityFilter extends FilterFunc {

    @Override
    public Boolean exec(final Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() == 0) {
            return false;
        }
        try {
            Object object = tuple.get(0);
            if (object == null) {
                return false;
            }
            int i = (Integer) object;
            return i == 0 || i == 1 || i == 4 || i == 5 || i == 9;
        } catch (ExecException e) {
            throw new IOException(e);
        }
    }
}
