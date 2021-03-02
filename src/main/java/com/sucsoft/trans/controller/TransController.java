package com.sucsoft.trans.controller;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.annotation.PostConstruct;
import java.io.*;
import java.math.BigDecimal;
import java.util.Base64;
import java.util.HashMap;

/**
 * @author luoyy
 * @date 2021/2/26 18:14
 */
@Controller
public class TransController {
    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    private final Logger logger = LoggerFactory.getLogger(Class.class);

    //@PostConstruct
    public void putTrans() {
        long start = System.currentTimeMillis();
        CSVReader csvReader = null;
        String basePah = System.getProperty("user.dir");
        logger.info("************ basePath is :" + basePah);
        File dir = new File(basePah);
        File[] files = dir.listFiles();
        int fileNo = 1;
        int sum = 0;
        for (File file : files) {
            if (file.getName().contains("csv")) {
                logger.info("********** fileRealPath is :" + file.getPath());
            logger.info("********** 第" + fileNo + "个文件开始录入......");
            try {
                long onceStart = System.currentTimeMillis();
                String path = file.getPath();
                csvReader = new CSVReader(new InputStreamReader(new FileInputStream(path), "gbk"));
                String[] strs;
                int index = 0;
                BulkRequest bulkRequest = new BulkRequest();
                while ((strs = csvReader.readNext()) != null) {
                    if (index > 0) {
                        bulkRequest.add(
                                new IndexRequest("eth_trans")
                                        .source(XContentType.JSON,
                                                "blockHash", strs[2],
                                                "blockNo", Long.parseLong(strs[3]),
                                                "time", strs[11],
                                                "txid", strs[0],
                                                "from", strs[5],
                                                "to", strs[6],
                                                "nonce", Integer.parseInt(strs[1]),
                                                "gasPrice", new BigDecimal(strs[9]),
                                                "gas", new BigDecimal(strs[8]),
                                                "value", new BigDecimal(strs[7])));
                    }
                    index++;
                    if (index % 2000 == 0) {
                        client.bulk(bulkRequest, RequestOptions.DEFAULT);
                        bulkRequest = new BulkRequest();
                    }
                }
                client.bulk(bulkRequest, RequestOptions.DEFAULT);
                long onceEnd = System.currentTimeMillis();
                index -= 1;
                sum += index;
                logger.info("********** 第" + fileNo + "个文件录入完成，耗时" + (onceEnd - onceStart) + "毫秒 ******" + "******录入数据" + index + "条******"
                + "*****文件名为：" + file.getName());
                fileNo++;
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (csvReader != null) {
                        csvReader.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
        }
        long end = System.currentTimeMillis();
        logger.info("============== 总条数 " + sum + "************");
        logger.info("============== 总耗时 " + (end - start) + "毫秒 *********");
    }

    //@PostConstruct
    public void putTransactions() {
        long start = System.currentTimeMillis();
        CSVReader csvReader = null;
        String basePah = System.getProperty("user.dir");
        logger.info("************ basePath is :" + basePah);
        File dir = new File(basePah);
        File[] files = dir.listFiles();
        int fileNo = 1;
        int sum = 0;
        for (File file : files) {
            if (file.getName().contains("csv")) {
                logger.info("********** fileRealPath is :" + file.getPath());
                logger.info("********** 第" + fileNo + "个文件开始录入......");
                try {
                    long onceStart = System.currentTimeMillis();
                    String path = file.getPath();
                    csvReader = new CSVReader(new InputStreamReader(new FileInputStream(path), "gbk"));
                    String[] strs;
                    int index = 0;
                    BulkRequest bulkRequest = new BulkRequest();
                    while ((strs = csvReader.readNext()) != null) {
                        if (index > 0) {
                            bulkRequest.add(
                                    new IndexRequest("eth_trans")
                                            .source(XContentType.JSON,
                                                    "blockHash", strs[2],
                                                    "blockNo", Long.parseLong(strs[3]),
                                                    "time", strs[11],
                                                    "txid", strs[0],
                                                    "from", strs[5],
                                                    "to", strs[6],
                                                    "nonce", Integer.parseInt(strs[1]),
                                                    "gasPrice", new BigDecimal(strs[9]),
                                                    "gas", new BigDecimal(strs[8]),
                                                    "value", new BigDecimal(strs[7])));
                        }
                        index++;
                        if (index % 2000 == 0) {
                            client.bulk(bulkRequest, RequestOptions.DEFAULT);
                            bulkRequest = new BulkRequest();
                        }
                    }
                    client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    long onceEnd = System.currentTimeMillis();
                    index -= 1;
                    sum += index;
                    logger.info("********** 第" + fileNo + "个文件录入完成，耗时" + (onceEnd - onceStart) + "毫秒 ******" + "******录入数据" + index + "条******"
                            + "*****文件名为：" + file.getName());
                    fileNo++;
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (csvReader != null) {
                            csvReader.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            }
        }
        long end = System.currentTimeMillis();
        logger.info("============== 总条数 " + sum + "************");
        logger.info("============== 总耗时 " + (end - start) + "毫秒 *********");
    }

    //@PostConstruct
    public void testPath(){
        String basePah = System.getProperty("user.dir");
        logger.info("************ basePath is :" + basePah);
        File dir = new File(basePah);
        File[] files = dir.listFiles();
        for (File file : files) {
            logger.info("********** fileRealPath is :" + file.getPath());
        }
    }

    /**
     * @author luoyy
     * @date 2021/2/25 15:48
     * 多文件批量插入数据
     */
    //@PostConstruct
    public void putBlock() throws IOException {
        long start = System.currentTimeMillis();
        CSVReader csvReader = null;
        String basePah = System.getProperty("user.dir");
        logger.info("************ basePath is :" + basePah);
        File dir = new File(basePah);
        File[] files = dir.listFiles();
        int fileNo = 1;
        int sum = 0;
        for (File file : files) {
            //循环遍历文件名列表，将其解析，插入
            if (file.getName().contains("block_")) {
            logger.info("********** fileRealPath is :" + file.getPath());
            logger.info("********** 第" + fileNo + "个文件开始录入......");
            try {
                long onceStart = System.currentTimeMillis();
                String path = file.getPath();
                csvReader = new CSVReader(new InputStreamReader(new FileInputStream(path), "gbk"));
                String[] strs;
                int index = 0;//记录插入的条数
                BulkRequest bulkRequest = new BulkRequest();
                while ((strs = csvReader.readNext()) != null) {
                    if (index > 0) {
                        bulkRequest.add(
                                new IndexRequest("eth_block")
                                        .source(XContentType.JSON,
                                                "number", Long.parseLong(strs[0]),
                                                "hash", strs[1],
                                                "parentHash", strs[2],
                                                "nonce", strs[3],
                                                "sha3Uncles", strs[4],
                                                "logsBloom", strs[5],
                                                "transactionsRoot", strs[6],
                                                "stateRoot", strs[7],
                                                "receiptsRoot", strs[8],
                                                "miner", strs[9],
                                                "difficulty", Double.parseDouble(strs[10]),
                                                "totalDifficulty", Double.parseDouble(strs[11]),
                                                "size", Long.parseLong(strs[12]),
                                                "extraData", strs[13],
                                                "gasLimit", Long.parseLong(strs[14]),
                                                "gasUsed", Long.parseLong(strs[15]),
                                                "timestamp", Long.parseLong(strs[16]),
                                                "transactionCount", Long.parseLong(strs[17])));
                    }
                    index++;
                    if (index % 2000 == 0) {
                        client.bulk(bulkRequest, RequestOptions.DEFAULT);
                        bulkRequest = new BulkRequest();
                    }
                }
                //执行批量插入请求
                client.bulk(bulkRequest, RequestOptions.DEFAULT);
                long onceEnd = System.currentTimeMillis();
                index -= 1;
                sum += index;
                logger.info("********** 第" + fileNo + "个文件录入完成，耗时" + (onceEnd - onceStart) + "毫秒 ******" + "******录入数据" + index + "条******"
                        + "*****文件名为：" + file.getName());
                fileNo++;
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (csvReader != null) {
                        csvReader.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        }
        long end = System.currentTimeMillis();
        logger.info("============== 总条数 " + sum + "************");
        logger.info("============== 总耗时 " + (end - start) + "毫秒 *********");
    }

    //@PostConstruct
    public void writeCsv() throws IOException {
        String basePah = System.getProperty("user.dir");
        CSVReader csvReader = null;
        File file = new File(basePah);
        String outputPath = basePah + File.separator + "numtime.csv";
        logger.info("**************" + outputPath + "**************");
        File outputFile = new File(outputPath);
        FileWriter writer = new FileWriter(outputFile);
        CSVWriter csvWriter = new CSVWriter(writer);
        File[] files = file.listFiles();
        for (File file1 : files) {
            //循环遍历文件名列表，将其解析，插入
            if (file1.getName().contains("block_")) {
                try {
                    String path = file1.getPath();
                    csvReader = new CSVReader(new InputStreamReader(new FileInputStream(path), "gbk"));
                    String[] strs;
                    int index = 0;//记录插入的条数
                    while ((strs = csvReader.readNext()) != null) {
                        if (index > 0) {
                            csvWriter.writeNext(new String[]{strs[0], strs[16]});
                        }
                        index++;
                    }
                    logger.info("===========" + (index - 1) + "===========");
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (csvReader != null) {
                            csvReader.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        logger.info("******************文件生成完毕******************");
        csvWriter.flush();
        csvWriter.close();
    }

    //将block表和usdt表根据number关联起来，并在usdt表中添加timestamp字段
    //@PostConstruct
    public void blockUsdt() throws IOException, CsvValidationException {
        long start = System.currentTimeMillis();
        String basePah = System.getProperty("user.dir");
        logger.info("************ basePath is :" + basePah);
        String outputPath = basePah + File.separator + "error.csv";
        logger.info("************ outputPath is :" + outputPath);
        File outputFile = new File(outputPath);
        FileWriter writer = new FileWriter(outputFile);
        CSVWriter csvWriter = new CSVWriter(writer);
        CSVReader csvReader = null;
        CSVReader csvReader1 = null;
        HashMap<String, String> map = new HashMap<>();
        String path1 = basePah + File.separator + "numtime.csv";
        logger.info("******** numtime.csv " + path1 + "********");
        csvReader1 = new CSVReader(new InputStreamReader(new FileInputStream(path1), "gbk"));
        String[] str1;
        while ((str1 = csvReader1.readNext()) != null) {
            map.put(str1[0], str1[1]);
        }
        File file = new File(basePah);
        File[] files = file.listFiles();//获取basePah文件夹下的文件名列表
        int fileNo = 1;
        int sum = 0;
        for (File file1 : files) {
            if (file1.getName().contains("usdt_")) {
                logger.info("********** fileRealPath is :" + file1.getPath());
                logger.info("********** 第" + fileNo + "个文件开始录入......");
                if (file1.length() == 0) {
                    logger.info("******** 第" + fileNo + "个文件为空");
                    fileNo++;
                    continue;
                }
                try {
                    long onceStart = System.currentTimeMillis();
                    String path = file1.getPath();
                    csvReader = new CSVReader(new InputStreamReader(new FileInputStream(path), "gbk"));
                    String[] strs;
                    int index = 0;
                    BulkRequest bulkRequest = new BulkRequest();
                    while ((strs = csvReader.readNext()) != null) {
                        if (index > 0) {
                            if (map.containsKey(strs[6])) {
                                bulkRequest.add(
                                        new IndexRequest("eth_usdt")
                                                .source(XContentType.JSON,
                                                        "tokenAddress", strs[0],
                                                        "from", strs[1],
                                                        "to", strs[2],
                                                        "value", Double.parseDouble(strs[3]),
                                                        "transactionHash", strs[4],
                                                        "logIndex", Long.parseLong(strs[5]),
                                                        "blockNo", Long.parseLong(strs[6]),
                                                        "timestamp", Long.parseLong(map.get(strs[6]))));
                            } else {
                                csvWriter.writeNext(new String[]{file1.getName(), strs[6]});
                            }
                        }
                        index++;
                        if (index % 2000 == 0) {
                            client.bulk(bulkRequest, RequestOptions.DEFAULT);
                            bulkRequest = new BulkRequest();
                        }
                    }
                    client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    long onceEnd = System.currentTimeMillis();
                    index -= 1;
                    sum += index;
                    logger.info("********** 第" + fileNo + "个文件录入完成，耗时" + (onceEnd - onceStart) + "毫秒 ******" + "******录入数据" + index + "条******"
                            + "*****文件名为：" + file1.getName());
                    fileNo++;
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (csvReader != null) {
                            csvReader.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        long end = System.currentTimeMillis();
        logger.info("============== 总条数 " + sum + "************");
        logger.info("============== 总耗时 " + (end - start) + "毫秒 *********");
    }

    //读取trans条数，看是否缺少条数
    @PostConstruct
    public void readTransNum() {
        long start = System.currentTimeMillis();
        CSVReader csvReader = null;
        String basePah = System.getProperty("user.dir");
        logger.info("************ basePath is :" + basePah);
        File dir = new File(basePah);
        File[] files = dir.listFiles();
        int fileNo = 1;
        int sum = 0;
        for (File file : files) {
            if (file.getName().contains("csv")) {
                logger.info("********** fileRealPath is :" + file.getPath());
                logger.info("********** 第" + fileNo + "个文件开始录入......");
                try {
                    long onceStart = System.currentTimeMillis();
                    String path = file.getPath();
                    csvReader = new CSVReader(new InputStreamReader(new FileInputStream(path), "gbk"));
                    String[] strs;
                    int index = 0;
                    //BulkRequest bulkRequest = new BulkRequest();
                    while ((strs = csvReader.readNext()) != null) {
//                        if (index > 0) {
//                            bulkRequest.add(
//                                    new IndexRequest("eth_trans")
//                                            .source(XContentType.JSON,
//                                                    "blockHash", strs[2],
//                                                    "blockNo", Long.parseLong(strs[3]),
//                                                    "time", strs[11],
//                                                    "txid", strs[0],
//                                                    "from", strs[5],
//                                                    "to", strs[6],
//                                                    "nonce", Integer.parseInt(strs[1]),
//                                                    "gasPrice", new BigDecimal(strs[9]),
//                                                    "gas", new BigDecimal(strs[8]),
//                                                    "value", new BigDecimal(strs[7])));
//                        }
                        index++;
//                        if (index % 2000 == 0) {
//                            client.bulk(bulkRequest, RequestOptions.DEFAULT);
//                            bulkRequest = new BulkRequest();
//                        }
                    }
                    //client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    long onceEnd = System.currentTimeMillis();
                    index -= 1;
                    sum += index;
                    logger.info("********** 第" + fileNo + "个文件录入完成，耗时" + (onceEnd - onceStart) + "毫秒 ******" + "******录入数据" + index + "条******"
                            + "*****文件名为：" + file.getName());
                    fileNo++;
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (csvReader != null) {
                            csvReader.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            }
        }
        long end = System.currentTimeMillis();
        logger.info("============== 总条数 " + sum + "************");
        logger.info("============== 总耗时 " + (end - start) + "毫秒 *********");
    }
}
