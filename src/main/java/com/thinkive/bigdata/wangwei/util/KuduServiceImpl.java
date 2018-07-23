package com.thinkive.bigdata.wangwei.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kudu.client.KuduException;

import java.io.Serializable;

public class KuduServiceImpl implements KuduService, Serializable {
    @Override
    public void doInsertProcess(String data) throws KuduException {

        JSONObject jsonObject = new JSONObject();
        KuduUtils.insert("impala::user.user_portrait_kudu", jsonObject);
    }
}
