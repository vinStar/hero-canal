package com.herohuang.doctor.service.impl;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.herohuang.doctor.dto.Canal;
import com.herohuang.doctor.service.CanalService;
import com.herohuang.doctor.util.PropertyUtil;
import com.herohuang.doctor.util.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Response;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created by herohuang on 26/04/2017.
 */
@Service
public class CanalServiceImpl implements CanalService {
    static Logger logger = LoggerFactory.getLogger(CanalServiceImpl.class);
    @Autowired
    private PropertyUtil propertyUtil;

    @Override
    public void startBinlog() throws Exception {//AddressUtils.getHostIp()
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("192.168.1.172",
                11111), "example", "", "");
        int batchSize = 1000;
        int emptyCount = 0;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            int totalEmptyCount = 10;
            while (true) {
                Message message = connector.getWithoutAck(batchSize);
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    try {
                        //logger.info("canal thread is running");
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    logger.info("canal message");
                    printEntry(message.getEntries());

                }
                connector.ack(batchId);
            }
        } finally {
            connector.disconnect();
        }
    }


    private void printEntry(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {

            if (!"test_a".equals(entry.getHeader().getSchemaName())) {
                //logger.info(entry.getHeader().getSchemaName());
                continue;
            }

            logger.info(entry.toString());

            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            CanalEntry.EventType eventType = rowChage.getEventType();
//            eventType == EventType.DELETE ||
//                    eventType == EventType.INSERT ||
//                    eventType == EventType.UPDATE ||
//                    eventType == EventType.ALTER
            String tableName = entry.getHeader().getTableName().toLowerCase();
            if (tableName.contains("tb_user")) {
                logger.info("eventType -- " + eventType.toString());

                logger.info("tableName -- " + tableName);
                List<Canal> list = propertyUtil.getCacheConfig();
                for (Canal canal : list) {
                    logger.info("canal config tableName -- " + canal.getTableName());
                    // database name + table name
                    String tableNameKey = entry.getHeader().getSchemaName() + "." + tableName;
                    if (canal.getTableName().toLowerCase().equals(tableNameKey)) {

                        String[] cacheKyes = canal.getCacheKey().split(",");

                        // 删除同步
//                        for (String cacheKye : cacheKyes) {
//                            RedisUtil.delKey("key" + cacheKye);
//                            logger.info(cacheKye);
//                        }


                        // redis lpush
                        try {
                            RedisUtil.lpushByte(tableNameKey.getBytes(), entry.toByteArray());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        //region ... redis lpop
//                        Response<byte[]> responseValue = RedisUtil.getPipeline(tableNameKey.getBytes());
//
//                        if (responseValue == null) continue;
//
//                        CanalEntry.Entry entryParseFromRedis = null;
//                        try {
//                            entryParseFromRedis = CanalEntry.Entry.parseFrom(responseValue.get());
//                        } catch (InvalidProtocolBufferException e) {
//                            e.printStackTrace();
//                        }
//                        String strColumn = null;
//
//                        try {
//                            strColumn = RowChange.parseFrom(entryParseFromRedis.getStoreValue()).
//                                    getRowDatasList().get(0).getAfterColumnsList().toString();
//                        } catch (InvalidProtocolBufferException e) {
//                            e.printStackTrace();
//                        }
//                        logger.info("getPipeline :  " + strColumn);
//
                        //endregion

                    }


                }
            }
        }
    }
}


