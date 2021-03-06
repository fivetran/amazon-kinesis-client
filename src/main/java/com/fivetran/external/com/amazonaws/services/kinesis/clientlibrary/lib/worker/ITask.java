/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.concurrent.Callable;

/**
 * Interface for shard processing tasks.
 * A task may execute an application callback (e.g. initialize, process, shutdown).
 */
interface ITask extends Callable<TaskResult> {

    /**
     * Perform task logic.
     * E.g. perform set up (e.g. fetch records) and invoke a callback (e.g. processRecords() API).
     * 
     * @return TaskResult (captures any exceptions encountered during execution of the task)
     */
    TaskResult call();

    /**
     * @return TaskType
     */
    TaskType getTaskType();

}
