package com.tulun.sparkF;

import java.util.HashMap;
import java.util.Map;

// 把入参的key-value对存到一个map里
public class ParseArgs {

    private Map<String, String> map = null;

    public ParseArgs(String[] args) {
        map = new HashMap<String, String>();

        if (args.length == 0) {
            return;
        }
        int i = 0;
        while (i < args.length) {
            String par = args[i].trim();
            if (par.startsWith("-")) {
                String key = par.substring(1).trim();
                i++;
                String value = null;
                if (args.length > i) {
                    value = args[i].trim();
                    if (value.startsWith("\"") || value.startsWith("\'")) {
                        value = value.substring(1, value.length() - 1).trim();
                    }
                }
                map.put(key, value);
                i++;
            } else {
                i++;
            }


        }


    }

    public Map<String, String> getMap() {
        return map;
    }

    public static void main(String[] args) {
        System.out.println();
    }
}
