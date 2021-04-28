package com.fivetran.external.com.amazonaws.services.testing;

import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSSessionCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.google.common.collect.ImmutableList;

import java.util.*;

public class DynamoTestUtils {

    static final long SAFE_CAPACITY_UNITS = 50L;
    static final long UNSAFE_WRITE_CAPACITY_UNITS = 1000L;
    private static final int MAX_INSERT_BATCH_SIZE = 24;
    public static final String REGION =  "us-east-1";
    private static final String ARN =  "arn:aws:iam::254359228911:role/FivetranDynamoDBTesting";
    private static final String EXTERNAL_ID =  "FivetranRocks";

    private static final String SIMPLE_TABLE_PARTITION_KEY = "partitionKey";
    private static final String SIMPLE_TABLE_SORT_KEY = "sortKey";
    private static final String SIMPLE_TABLE_DATA_COL = "data";

    static final AmazonDynamoDB DYNAMO_CLIENT = createDynamoClient();
    static final AmazonDynamoDBStreamsAdapterClient STREAMS_CLIENT = createStreamsClient();

    public static AmazonDynamoDB createDynamoClient(){
        return AmazonDynamoDBClientBuilder.standard()
                .withCredentials(awsSessionCredentialsProvider())
                .withRegion(Regions.fromName(REGION))
                .build();
    }

    public static AmazonDynamoDBStreamsAdapterClient createStreamsClient(){
        return new AmazonDynamoDBStreamsAdapterClient(awsSessionCredentialsProvider());
    }

    public static AWSSessionCredentialsProvider awsSessionCredentialsProvider() {
        return new AWSSessionCredentialsProvider() {
            @Override
            public AWSSessionCredentials getCredentials() {
                return getBasicSessionCredentials();
            }

            @Override
            public void refresh() {}

            private BasicSessionCredentials getBasicSessionCredentials() {
                AssumeRoleRequest assumeRoleRequest =
                        new AssumeRoleRequest()
                                .withExternalId(EXTERNAL_ID)
                                .withRoleArn(ARN)
                                .withRoleSessionName("Fivetran-Worker-Debug-Session");
                Credentials roleCredentials;
                roleCredentials =
                        AWSSecurityTokenServiceClientBuilder.standard()
                                                .withRegion(Regions.fromName(REGION))
                                                .build()
                                                .assumeRole(assumeRoleRequest)
                                                .getCredentials();

                assert roleCredentials != null : "Could not fetch role creds from AWS Security Token Service";
                return new BasicSessionCredentials(
                        roleCredentials.getAccessKeyId(),
                        roleCredentials.getSecretAccessKey(),
                        roleCredentials.getSessionToken());
            }
        };
    }

    public static TableDescription describeTable(String tableName) {
        DescribeTableResult result = DYNAMO_CLIENT.describeTable(new DescribeTableRequest().withTableName(tableName));
        return result.getTable();
    }

    public static void createTable(
            String tableName,
            boolean enableMultiSharding,
            List<KeySchemaElement> keySchema,
            List<AttributeDefinition> attributeDefinitions) {

        ProvisionedThroughput provisionedThroughput =
                new ProvisionedThroughput()
                        .withReadCapacityUnits(SAFE_CAPACITY_UNITS)
                        .withWriteCapacityUnits(
                                (enableMultiSharding ? UNSAFE_WRITE_CAPACITY_UNITS : SAFE_CAPACITY_UNITS));
        StreamSpecification streamSpecification =
                new StreamSpecification().withStreamEnabled(true).withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES);
        CreateTableRequest createTableRequest =
                new CreateTableRequest()
                        .withTableName(tableName)
                        .withAttributeDefinitions(attributeDefinitions)
                        .withKeySchema(keySchema)
                        .withProvisionedThroughput(provisionedThroughput)
                        .withStreamSpecification(streamSpecification);

        try {
            DYNAMO_CLIENT.createTable(createTableRequest);
            awaitTableCreation(tableName);
        } catch (com.amazonaws.services.kinesis.model.ResourceInUseException e) {
            throw new RuntimeException("Table already exists.");
        }
    }

    public static boolean createSimpleTable(String tableName) {
        KeySchemaElement pkeySchemaElement = new KeySchemaElement().withAttributeName(SIMPLE_TABLE_PARTITION_KEY).withKeyType(KeyType.HASH);
        KeySchemaElement sortKeySchemaElement = new KeySchemaElement().withAttributeName(SIMPLE_TABLE_SORT_KEY).withKeyType(KeyType.RANGE);

        List<KeySchemaElement> keySchema = ImmutableList.of(pkeySchemaElement, sortKeySchemaElement);

        AttributeDefinition pkeyAttribute = new AttributeDefinition()
                .withAttributeName(SIMPLE_TABLE_PARTITION_KEY)
                .withAttributeType(ScalarAttributeType.S);

        AttributeDefinition sortKeyAttribute = new AttributeDefinition()
                .withAttributeName(SIMPLE_TABLE_SORT_KEY)
                .withAttributeType(ScalarAttributeType.S);

        List<AttributeDefinition> attributes = ImmutableList.of(pkeyAttribute, sortKeyAttribute);

        // act
        DynamoTestUtils.createTable(tableName, false, keySchema, attributes);
        return DynamoTestUtils.tableExists(tableName);
    }


    private static void awaitTableCreation(String tableName) {
        for (int retries = 0; retries < 100; retries++) {
            TableDescription description = describeTable(tableName);
            boolean created = description.getTableStatus().equals("ACTIVE");
            if (created) {
                return;
            } else {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // do nothing
                }
            }
        }
    }

    public static void deleteTable(String table) {
        try {
            DYNAMO_CLIENT.deleteTable(new DeleteTableRequest().withTableName(table));
            awaitTableDeletion(table);
        } catch (ResourceNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void awaitTableDeletion(String tableName) {
        for (int retries = 0; retries < 100; retries++) {
            try {
                TableDescription description = describeTable(tableName);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } catch (ResourceNotFoundException e) {
                return;
            }
        }
    }

    public static boolean tableExists(String tableName) {
        boolean exists = true;
        try {
            describeTable(tableName);
        } catch (ResourceNotFoundException e) {
            exists = false;
        }

        return exists;
    }

    public static List<Item> fillSimpleTable(String tableName, Long rows) {
        List<Item> allItems = new ArrayList<>();
        List<Item> items = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            items.add(createSimpleItemInPartition(i % 24));

            if (i % MAX_INSERT_BATCH_SIZE == 0) {
                insertItems(tableName, items);
                allItems.addAll(items);
                items.clear();
            }
        }
        insertItems(tableName, items);
        allItems.addAll(items);
        return allItems;
    }

    public static void insertItems(String tableName, List<Item> items) {
        DynamoDB wrappedClient = new DynamoDB(DYNAMO_CLIENT);
        List<Item> batch = new ArrayList<>();
        for (Item item : items) {
            batch.add(item);
            if (batch.size() == MAX_INSERT_BATCH_SIZE) {
                insertBatch(tableName, batch, wrappedClient);
                batch.clear();
            }
        }
    }

    private static void insertBatch(String tableName, List<Item> batch, DynamoDB wrappedClient) {
        try {
            TableWriteItems tableWriteItems = new TableWriteItems(tableName);
            tableWriteItems.withItemsToPut(batch);

            BatchWriteItemOutcome outcome = wrappedClient.batchWriteItem(tableWriteItems);

            int tries = 0;
            while (!outcome.getUnprocessedItems().isEmpty()) {
                StringBuilder sb = new StringBuilder();
                outcome.getUnprocessedItems()
                        .forEach((k, wrs) -> sb.append(k).append("->").append(wrs.size()).append("\n"));

                Thread.sleep(250L * (2 ^ tries++));

                System.out.println(
                        "Couldn't write everything in one go. Unprocessed write requests: "
                                + sb.toString()
                                + ". Trying again...");
                outcome = wrappedClient.batchWriteItemUnprocessed(outcome.getUnprocessedItems());
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void deleteItems(String tableName, List<Item> items) {
        DynamoDB wrappedClient = new DynamoDB(DYNAMO_CLIENT);
        List<Item> batch = new ArrayList<>();
        for (Item item : items) {
            batch.add(item);
            if (batch.size() == MAX_INSERT_BATCH_SIZE) {
                deleteBatch(tableName, batch, wrappedClient);
                batch.clear();
            }
        }
    }

    public static void deleteBatch(String tableName, List<Item> items, DynamoDB wrappedClient) {
        try {
            TableWriteItems tableWriteItems = new TableWriteItems(tableName);
            tableWriteItems.withItemsToPut(items);

            BatchWriteItemOutcome outcome = wrappedClient.batchWriteItem(tableWriteItems);

            int tries = 0;
            while (!outcome.getUnprocessedItems().isEmpty()) {
                StringBuilder sb = new StringBuilder();
                outcome.getUnprocessedItems()
                        .forEach((k, wrs) -> sb.append(k).append("->").append(wrs.size()).append("\n"));

                Thread.sleep(250L * (2 ^ tries++));

                System.out.println(
                        "Couldn't write everything in one go. Unprocessed write requests: "
                                + sb.toString()
                                + ". Trying again...");
                outcome = wrappedClient.batchWriteItemUnprocessed(outcome.getUnprocessedItems());
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    private static Item createSimpleItemInPartition(int partition) {
        Item item = new Item();
        return item.withString(SIMPLE_TABLE_PARTITION_KEY, String.valueOf(partition))
                .withString(SIMPLE_TABLE_SORT_KEY, UUID.randomUUID().toString())
                .withString(SIMPLE_TABLE_DATA_COL, UUID.randomUUID().toString());
    }

    public static Item updateSimpleItem(Item oldItem) {
        Item item = new Item();
        return item.withString(SIMPLE_TABLE_PARTITION_KEY, oldItem.getString(SIMPLE_TABLE_PARTITION_KEY))
                .withString(SIMPLE_TABLE_SORT_KEY, oldItem.getString(SIMPLE_TABLE_DATA_COL))
                .withString(SIMPLE_TABLE_DATA_COL, UUID.randomUUID().toString());
    }

    public static String getStreamArn(String tableName) {
        return DynamoTestUtils.describeTable(tableName).getLatestStreamArn();
    }

    public static List<com.amazonaws.services.kinesis.model.Shard> getShardsForTable(String tableName) {
        TableDescription description = DynamoTestUtils.describeTable(tableName);
        String streamARN = description.getLatestStreamArn();

        return getShardsForStream(streamARN);
    }

    public static List<com.amazonaws.services.kinesis.model.Shard> getShardsForStream(String streamARN) {
        return STREAMS_CLIENT.describeStream(streamARN).getStreamDescription().getShards();
    }
}
