package com.alibaba.maxgraph.v2.frontend.utils;

public class WriteSessionUtil {

    public static String createWriteSession(int nodeId, long clientIdx, long timestamp) {
        return nodeId + "-" + clientIdx + "-" + timestamp;
    }

    public static long getClientIdx(String session) {
        String[] items = session.split("-");
        return Long.valueOf(items[1]);
    }
}
