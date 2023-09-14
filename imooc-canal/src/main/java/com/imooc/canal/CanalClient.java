package com.imooc.canal;

import com.alibaba.fastjson.JSON;
import com.alibaba.google.common.base.CaseFormat;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;

/**
 * 通过TCP的方式拉取数据 mysql==>canal(tcp)==>canal client ==> kafka
 *
 * MMM
 */
public class CanalClient {

    public static void main(String[] args) throws Exception {

        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("ruozedata001", 11111),
                "example",
                null, null);

        while(true) {
            connector.connect();

            connector.subscribe("pkdb.*");
            Message message = connector.get(100);

            List<CanalEntry.Entry> entries = message.getEntries();
            if(!entries.isEmpty()) {
                for (CanalEntry.Entry entry : entries) {

                    String tableName = entry.getHeader().getTableName();


                    CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                    // insert update delete...
                    CanalEntry.EventType eventType = rowChange.getEventType();

                    if(eventType == CanalEntry.EventType.INSERT) {
                        for (CanalEntry.RowData rowData : rowDatasList) {
                            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

                            HashMap<String, String> map = new HashMap<>();

                            for (CanalEntry.Column column : afterColumnsList) {
                                String key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                                map.put(key, column.getValue());
                            }


                            /**
                             * TODO...
                             * 如果到这一步ok，下面要做的事情不就是把map的数据发送到你们想要的地方去么...
                             */
                            System.out.println("tableName:"+ tableName + " , " + JSON.toJSONString(map));
                        }
                    }
                }
            }
        }
    }
}
