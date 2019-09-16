package org.apache.spark.sql.dbPartiiton.service;

import java.util.HashMap;

public class ResConx {
    static ResConx instance = new ResConx();
    protected HashMap<String, ResPool> pools = new HashMap<>();
    public static ResConx getInstance() {
        return instance;
    }

   // 针对每一个数据库的连接池
    synchronized public ResPool conx(String url) {
        if (pools.containsKey(url))
            return pools.get(url);
        else {
            ResPool conxIns = new ResPool();
            pools.put(url, conxIns);
            return conxIns;
        }
    }
}
