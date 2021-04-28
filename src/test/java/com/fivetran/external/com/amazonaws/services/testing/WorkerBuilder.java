package com.fivetran.external.com.amazonaws.services.testing;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSSessionCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

import java.util.Optional;

public class WorkerBuilder {

    private static final String LEASE_TABLE = "benv_least_table";

    private KinesisClientLibConfiguration workerConfig;
    private AmazonDynamoDBStreamsAdapterClient dynamoKinesisAdapterClient;
    private SimpleStreamsRecordProcessor recordProcessor;
    private AmazonCloudWatch cloudWatchClient;
    private AmazonDynamoDB dynamoClient;

    public WorkerBuilder(String region,
                         String table,
                         String streamARN,
                         AWSSessionCredentialsProvider sessionCredentials,
                         AmazonDynamoDB dynamoClient,
                         AmazonCloudWatch cloudWatchClient,
                         String workerName,
                         SimpleStreamsRecordProcessor recordProcessor) {
        this(region, table, streamARN, sessionCredentials, dynamoClient, cloudWatchClient, Optional.empty(), Optional.empty(), workerName, recordProcessor);
    }

    public WorkerBuilder(String region,
                         String table,
                         String streamARN,
                         AWSSessionCredentialsProvider sessionCredentials,
                         AmazonDynamoDB dynamoClient,
                         AmazonCloudWatch cloudWatchClient,
                         Optional<Integer> shardsLimit,
                         Optional<Integer> maxConnections,
                         String workerName,
                         SimpleStreamsRecordProcessor recordProcessor) {
        this.workerConfig = createWorkerConfig(table, streamARN, sessionCredentials, region, workerName, shardsLimit);
        this.dynamoKinesisAdapterClient = createKinesisClient(sessionCredentials, maxConnections, region);
        this.recordProcessor = recordProcessor;
        this.dynamoClient = dynamoClient;
        this.cloudWatchClient = cloudWatchClient;
    }

    private KinesisClientLibConfiguration createWorkerConfig(
            String table, String streamARN, AWSSessionCredentialsProvider sessionCredentials, String region, String workerName, Optional<Integer> shardsLimit) {
        KinesisClientLibConfiguration workerConfig =
                new KinesisClientLibConfiguration(LEASE_TABLE, streamARN, sessionCredentials, workerName)
                        .withCallProcessRecordsEvenForEmptyRecordList(true)
                        .withRegionName(region)
                        .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

        if (shardsLimit.isPresent()) {
            workerConfig = workerConfig.withMaxLeasesForWorker(shardsLimit.get());
        }

        return workerConfig;
    }

    private AmazonDynamoDBStreamsAdapterClient createKinesisClient(AWSSessionCredentialsProvider sessionCredentials, Optional<Integer> maxConnections, String region) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        maxConnections.ifPresent(max -> clientConfiguration.setMaxConnections(max));

        AmazonDynamoDBStreamsAdapterClient dynamoKinesisAdapterClient =
                new AmazonDynamoDBStreamsAdapterClient(sessionCredentials, clientConfiguration);
        dynamoKinesisAdapterClient.setRegion(Region.getRegion(Regions.fromName(region)));
        return dynamoKinesisAdapterClient;
    }


    public Worker build() {
        return new Worker.Builder()
                .config(workerConfig)
                .kinesisClient(dynamoKinesisAdapterClient)
                .dynamoDBClient(dynamoClient)
                .cloudWatchClient(cloudWatchClient)
                .recordProcessorFactory(() -> recordProcessor)
                .exitOnFailure()
                .build();
    }
}
