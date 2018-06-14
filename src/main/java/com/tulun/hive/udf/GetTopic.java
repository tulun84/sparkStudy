package com.tulun.hive.udf;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 获取专题name
 * 用法：getTopic(URL)
 */
public class GetTopic extends UDF {

    public String evaluate(final String url) {
        if (url == null || url.trim().length() == 0) {
            return null;
        }
        Pattern p = Pattern.compile("zhuanti/([a-zA-Z0-9]+)");
        Matcher m = p.matcher(url);
        if (m.find()) {
            return m.group(0).toLowerCase().split("\\/")[1];
        }
        return null;
    }

    public int evaluate(int type) {
        return 0;
    }


    public static void main(String[] argc) {
        GetTopic gh = new GetTopic();
        System.out.println(gh.evaluate("http://www.yhd.com/zhuanti/qingREnj3ie"));
    }

}
