package com.thinkive.bigdata.wangwei.util;

import org.apache.kudu.client.KuduException;

public interface KuduService {
    /**
     * kudu插入
     * @param data
     * @throws KuduException
     */
    void doInsertProcess(String data) throws KuduException;
}
