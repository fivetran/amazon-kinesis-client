package com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.Record;

import com.amazonaws.services.kinesis.model.Shard;
import com.fivetran.external.com.amazonaws.services.testing.DynamoTestUtils;
import com.fivetran.external.com.amazonaws.services.testing.MetricInterceptorCloudWatch;
import com.fivetran.external.com.amazonaws.services.testing.SimpleStreamsRecordProcessor;
import com.fivetran.external.com.amazonaws.services.testing.WorkerBuilder;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class WorkerEndToEndTest {

    private static final boolean PRESERVE_TABLE = true;
    private static final String DEBUG_TABLE = "benv_worker_debug";

    private Worker configureWorker(Consumer<Record> recordConsumer, Optional<Integer> shardsLimit) {
        String streamARN = DynamoTestUtils.getStreamArn(DEBUG_TABLE);
        AmazonDynamoDB dynamoClient = DynamoTestUtils.createDynamoClient();
        AmazonCloudWatch cloudWatchClient = new MetricInterceptorCloudWatch();
        String workerName = UUID.randomUUID().toString();

        SimpleStreamsRecordProcessor streamsRecordProcessor = new SimpleStreamsRecordProcessor(recordConsumer);

        System.out.println("worker name: " + workerName);

        return new WorkerBuilder(DynamoTestUtils.REGION, DEBUG_TABLE, streamARN, DynamoTestUtils.awsSessionCredentialsProvider(), dynamoClient, cloudWatchClient, shardsLimit, Optional.empty(), workerName, streamsRecordProcessor).build();
    }

    @BeforeClass
    public static void beforeClass() {
        if (!DynamoTestUtils.tableExists(DEBUG_TABLE)) {
            DynamoTestUtils.createSimpleTable(DEBUG_TABLE);
            generateShards(2);
        }
    }

//    @AfterClass
//    public static void afterClass() {
//        if (!PRESERVE_TABLE && DynamoTestUtils.tableExists(DEBUG_TABLE)) DynamoTestUtils.deleteTable(DEBUG_TABLE);
//    }

    private static int generateShards(int targetNumShards) {
        List<Shard> shards = new ArrayList<>();
        for (int i = 0; i < 10 && shards.size() < targetNumShards; i++) {
            generatesShardsOp();
            shards = DynamoTestUtils.getShardsForTable(DEBUG_TABLE);
            System.out.printf("There are %d shards in %s", shards.size(), DEBUG_TABLE);
        }
        return shards.size();
    }

    private static void generatesShardsOp() {
        long numRows = 1000L;
        List<Item> items = DynamoTestUtils.fillSimpleTable(DEBUG_TABLE, numRows);
        List<Item> updatedItems = items.stream().map(DynamoTestUtils::updateSimpleItem).collect(Collectors.toList());
        DynamoTestUtils.insertItems(DEBUG_TABLE, updatedItems);
        DynamoTestUtils.deleteItems(DEBUG_TABLE, items);
    }

    @Test
    public void endToEnd_LimitedShards() {
        List<Record> records = new ArrayList<>();
        Optional<Integer> shardsLimit = Optional.of(15);

        int extantShards = DynamoTestUtils.getShardsForTable(DEBUG_TABLE).size();
        Assert.assertTrue(extantShards > 1);
        Worker worker = configureWorker(records::add, shardsLimit);

         worker.run();
    }
}
