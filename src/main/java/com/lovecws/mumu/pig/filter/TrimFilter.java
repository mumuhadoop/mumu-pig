package com.lovecws.mumu.pig.filter;

import org.apache.pig.PrimitiveEvalFunc;

import java.io.IOException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 去除空格过滤器
 * @date 2017-10-20 15:10
 */
public class TrimFilter extends PrimitiveEvalFunc<String, String> {
    @Override
    public String exec(final String value) throws IOException {
        if (value == null) {
            return null;
        }
        return value.trim();
    }
}
