package com.adshiye.bigdata.util.other;

import org.apache.kudu.client.KuduException;

public interface KuduService {
    /**
     * kudu插入
     * @param data
     * @throws KuduException
     */
    void doInsertProcess(String data) throws KuduException;
}
