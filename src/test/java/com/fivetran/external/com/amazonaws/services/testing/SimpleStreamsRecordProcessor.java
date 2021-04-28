package com.fivetran.external.com.amazonaws.services.testing;

import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IShutdownNotificationAware;
import com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.lib.worker.StreamRecordProcessingError;
import com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class SimpleStreamsRecordProcessor implements IRecordProcessor, IShutdownNotificationAware {

    private static final int CHECKPOINT_RETRIES = 3;
    private static final int PROCESS_RECORDS_RETRIES = 3;

    private final ReentrantLock lock;
    private final AtomicInteger totalRecordsSynced;
    private Consumer<Record> recordConsumer;

    public SimpleStreamsRecordProcessor(Consumer<Record> recordConsumer){
        this.lock = new ReentrantLock();
        this.totalRecordsSynced = new AtomicInteger();
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        String kinesisShardId = initializationInput.getShardId();

        System.out.println("Initialized shard: " + kinesisShardId);
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        for (int attempt = 0; /* see catch */ ; attempt++) {
            String shardId = "unknown";
            try {
                shardId = processRecordsInput.getShardId();
                int recordsProcessed = processRecordsInput.getRecords().size();
                IRecordProcessorCheckpointer checkpointer = processRecordsInput.getCheckpointer();

                for (com.amazonaws.services.kinesis.model.Record kinesisRecord : processRecordsInput.getRecords()) {
                    if (!(kinesisRecord instanceof RecordAdapter))
                        // This record processor is not being used with the DynamoDB Streams Adapter!
                        throw new IllegalArgumentException(
                                "Record is not an instance of RecordAdapter: " + kinesisRecord);

                    RecordAdapter dynamoRecord = (RecordAdapter) kinesisRecord;

                    try {
                        lock.lock();
                        recordConsumer.accept(dynamoRecord.getInternalObject());
                    } finally {
                        lock.unlock();
                    }
                }

                totalRecordsSynced.addAndGet(recordsProcessed);

                checkpoint(checkpointer);
                return;
            } catch (ShutdownException e) {
                System.out.println(
                        String.format(
                                "shardId: %s; RecordProcessor has been shutdown, exiting method cleanly; cause: %s",
                                shardId, e.getMessage()));
                return;
            } catch (Exception t) {
                // we throw an error here so that it will bubble back up to Worker#runProcessLoop
                if (attempt >= PROCESS_RECORDS_RETRIES) {
                    System.out.println(
                            String.format(
                                    "shardId: %s; RecordProcessor failing after %d tries; cause: %s",
                                    shardId, PROCESS_RECORDS_RETRIES, t.getMessage()));
                    throw new StreamRecordProcessingError(shardId);
                }

                final String finalizedShardId = shardId;
                // Backoff and re-attempt checkpoint upon transient failures
                Consumer<Long> sleepLog =
                        sleepInSeconds ->
                                System.out.println(
                                        "shardId: "
                                                + finalizedShardId
                                                + "; Exception when processing records. Retrying after "
                                                + sleepInSeconds
                                                + " seconds...");
                sleepThreadBackoff(finalizedShardId, attempt, sleepLog);
            }
        }
    }

    private static void checkpoint(IRecordProcessorCheckpointer checkpointer) throws ShutdownException {
        System.out.println("Checkpointing...");

        for (int attempt = 0; /* see catch */ ; attempt++) {
            try {
                checkpointer.checkpoint();
                return;
            } catch (ThrottlingException | KinesisClientLibDependencyException e) {
                if (attempt >= CHECKPOINT_RETRIES) throw new RuntimeException(e);

                // Backoff and re-attempt checkpoint upon transient failures
                Consumer<Long> sleepLog =
                        sleepInSeconds ->
                                System.out.println(
                                        "Transient issue when checkpointing. Retrying after "
                                                + sleepInSeconds
                                                + " seconds...");
                sleepThreadBackoff("", attempt, sleepLog);
            } catch (InvalidStateException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static void sleepThreadBackoff(String id, int attempt, Consumer<Long> sleepLog) {
        try {
            Duration sleepDuration = calculateDuration(attempt);
            sleepLog.accept(sleepDuration.getSeconds());
            Thread.sleep(sleepDuration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println(id.isEmpty() ? "" : String.format("id: %s; ", id) + "Sleep interrupted: " + e.getMessage());
        }
    }

    static Duration calculateDuration(int attempt) {
        long baseSeconds = (long) Math.pow(2, attempt) * 60;
        int randomSeconds = ThreadLocalRandom.current().nextInt(-30, 90);
        return Duration.ofSeconds(baseSeconds + randomSeconds);
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
            System.out.println("Dynamo record stream shutdown. Checkpointing one last time");
            try {
                checkpoint(shutdownInput.getCheckpointer());
            } catch (ShutdownException e) {
                throw new RuntimeException(e);
            }
        } else {
            System.out.println(
                    "Unexpected shutdown reason: " + shutdownInput.getShutdownReason() + ". We will not checkpoint");
        }
    }

    @Override
    public void shutdownRequested(IRecordProcessorCheckpointer checkpointer) {
        System.out.println("Dynamo record stream shutdown requested. Checkpointing one last time...");

        try {
            checkpoint(checkpointer);
        } catch (ShutdownException e) {
            throw new RuntimeException(e);
        }
    }
}
