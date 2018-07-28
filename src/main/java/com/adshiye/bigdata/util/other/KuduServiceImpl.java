package com.adshiye.bigdata.util.other;

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
