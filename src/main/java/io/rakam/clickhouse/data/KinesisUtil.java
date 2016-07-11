package io.rakam.clickhouse.data;

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import io.airlift.log.Logger;

public class KinesisUtil
{
    final static Logger LOGGER = Logger.get(KinesisUtil.class);

    /**
     * Creates an Amazon Kinesis stream if it does not exist and waits for it to become available
     *
     * @param kinesisClient The {@link com.amazonaws.services.kinesis.AmazonKinesisClient} with Amazon Kinesis read and write privileges
     * @param streamName The Amazon Kinesis stream name to create
     * @param shardCount The shard count to create the stream with
     * @throws IllegalStateException Invalid Amazon Kinesis stream state
     * @throws IllegalStateException Stream does not go active before the timeout
     */
    public static void createAndWaitForStreamToBecomeAvailable(AmazonKinesisClient kinesisClient,
            String streamName,
            int shardCount)
    {
        if (streamExists(kinesisClient, streamName)) {
            String state = streamState(kinesisClient, streamName);
            switch (state) {
                case "DELETING":
                    long startTime = System.currentTimeMillis();
                    long endTime = startTime + 1000 * 120;
                    while (System.currentTimeMillis() < endTime && streamExists(kinesisClient, streamName)) {
                        try {
                            LOGGER.info("...Deleting Stream " + streamName + "...");
                            Thread.sleep(1000 * 10);
                        }
                        catch (InterruptedException e) {
                        }
                    }
                    if (streamExists(kinesisClient, streamName)) {
                        LOGGER.error("KinesisUtils timed out waiting for stream " + streamName + " to delete");
                        throw new IllegalStateException("KinesisUtils timed out waiting for stream " + streamName
                                + " to delete");
                    }
                case "ACTIVE":
                    LOGGER.info("Stream " + streamName + " is ACTIVE");
                    return;
                case "CREATING":
                    break;
                case "UPDATING":
                    LOGGER.info("Stream " + streamName + " is UPDATING");
                    return;
                default:
                    throw new IllegalStateException("Illegal stream state: " + state);
            }
        }
        else {
            CreateStreamRequest createStreamRequest = new CreateStreamRequest();
            createStreamRequest.setStreamName(streamName);
            createStreamRequest.setShardCount(shardCount);
            kinesisClient.createStream(createStreamRequest);
            LOGGER.info("Stream " + streamName + " created");
        }
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(1000 * 10);
            }
            catch (Exception e) {
            }
            try {
                String streamStatus = streamState(kinesisClient, streamName);
                if (streamStatus.equals("ACTIVE")) {
                    LOGGER.info("Stream " + streamName + " is ACTIVE");
                    return;
                }
            }
            catch (ResourceNotFoundException e) {
                throw new IllegalStateException("Stream " + streamName + " never went active");
            }
        }
    }

    /**
     * Return the state of a Amazon Kinesis stream.
     *
     * @param kinesisClient The {@link com.amazonaws.services.kinesis.AmazonKinesisClient} with Amazon Kinesis read privileges
     * @param streamName The Amazon Kinesis stream to get the state of
     * @return String representation of the Stream state
     */
    private static String streamState(AmazonKinesisClient kinesisClient, String streamName)
    {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);
        try {
            return kinesisClient.describeStream(describeStreamRequest).getStreamDescription().getStreamStatus();
        }
        catch (AmazonServiceException e) {
            return null;
        }
    }

    /**
     * Helper method to determine if an Amazon Kinesis stream exists.
     *
     * @param kinesisClient The {@link com.amazonaws.services.kinesis.AmazonKinesisClient} with Amazon Kinesis read privileges
     * @param streamName The Amazon Kinesis stream to check for
     * @return true if the Amazon Kinesis stream exists, otherwise return false
     */
    private static boolean streamExists(AmazonKinesisClient kinesisClient, String streamName)
    {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);
        try {
            kinesisClient.describeStream(describeStreamRequest);
            return true;
        }
        catch (ResourceNotFoundException e) {
            return false;
        }
    }
}
