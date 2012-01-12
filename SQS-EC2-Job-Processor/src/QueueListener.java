/*
 * Copyright 2008 Amazon Technologies, Inc.  Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://aws.amazon.com/apache2.0
 *
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package de.tuhh.parallel.cw.jobprocessor;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.xerox.amazonws.sqs2.Message;
import com.xerox.amazonws.sqs2.MessageQueue;
import com.xerox.amazonws.sqs2.SQSException;
import com.xerox.amazonws.sqs2.SQSUtils;

/**
 * This class demonstrates idiomatic, single-threaded use of SQS in a job
 * processing loop using the Typica library.
 * 
 * The basic work-flow is:
 * 
 * <pre>
 * - Receive a single message from the jobs backlog queue whose body is expected
 *     to contain a job definition
 * - Process the job by handing the job definition to a JobExecutor
 * - If the job succeeds, delete the job from the jobs backlog and place the
 *     result in the results queue
 * - Otherwise, keep track of the number of times a job has failed, moving it
 *     to a dead letter (error) queue if it fails jobFailureRetries times
 * </pre>
 * 
 * To run your own job execution logic, subclass JobExecutor, add it to the
 * classpath, and specify it on the commandline (or provide to the constructor,
 * if you're instantiating your own QueueListener object).
 * 
 * Note: The Typica library base64 encodes and decodes messages, so if you're
 * using another library to interact with this QueueListener, be sure to do the
 * same.
 * 
 * @author walters
 * 
 */
public class QueueListener {

    private static Logger log = Logger.getLogger(QueueListener.class);

    public static void main(String[] args) {
        try {
            // Parse commandline into QueueListener object
            QueueListener ql = parseArgs(args);
            // Start it listening and processing jobs
            ql.start();
        } catch (Exception e) {
            log.error("A problem occurred", e);
        }
    }

    /**
     * Prints commandline usage information.
     */
    private static void printUsage() {
        System.err.println("usage: QueueListener <access key id> <secret access key> "
                + "<job backlog queue name> <job results queue name> "
                + "<dead letter queue name> <poll rate per minute> "
                + "<expected job execution time in seconds> <job failure retries> "
                + "<fully-qualified JobExecutor subclass>");
    }

    /**
     * Parses arguments from the commandline, connects to the specified queues
     * (creating them if they don't already exist), and returns a QueueListener
     * object.
     * 
     * @param args
     *            commandline arguments
     * @return the corresponding QueueListener
     * @throws SQSException
     *             if there is an error connecting to the specified queues
     */
    private static QueueListener parseArgs(String[] args) throws SQSException {
        if (args.length < 9) {
            printUsage();
            System.exit(-1);
        }
        String awsAccessKeyId = null;
        String secretAccessKey = null;
        String jobBacklogQueueName = null;
        String jobResultsQueueName = null;
        String deadLetterQueueName = null;
        double pollRatePerMin = 0;
        int expectedJobExecutionTimeInSecs = 0;
        int jobFailureRetries = 0;
        JobExecutor jobExecutor = null;
        try {
            awsAccessKeyId = args[0];
            secretAccessKey = args[1];
            jobBacklogQueueName = args[2];
            jobResultsQueueName = args[3];
            deadLetterQueueName = args[4];
            pollRatePerMin = Double.parseDouble(args[5]);
            expectedJobExecutionTimeInSecs = Integer.parseInt(args[6]);
            jobFailureRetries = Integer.parseInt(args[7]);
            try {
                jobExecutor = (JobExecutor) Class.forName(args[8]).newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException("JobExecutor subclass " + args[8] + " "
                        + e.getClass().getCanonicalName());
            }
        } catch (Exception e) {
            log.error("Problem parsing commandline argument " + e.getMessage());
            printUsage();
            System.exit(-1);
        }

        // Setup message queues, creating them if they don't exist
        log.info("Connecting to job backlog queue " + jobBacklogQueueName);
        MessageQueue backlogQueue = SQSUtils.connectToQueue(jobBacklogQueueName, awsAccessKeyId,
                secretAccessKey);
        log.info("Connecting to job results queue " + jobResultsQueueName);
        MessageQueue resultsQueue = SQSUtils.connectToQueue(jobResultsQueueName, awsAccessKeyId,
                secretAccessKey);
        log.info("Connecting to dead letter queue " + deadLetterQueueName);
        MessageQueue deadLetterQueue = SQSUtils.connectToQueue(deadLetterQueueName, awsAccessKeyId,
                secretAccessKey);

        // Create the QueueListener
        return new QueueListener(backlogQueue, resultsQueue, deadLetterQueue, pollRatePerMin,
                expectedJobExecutionTimeInSecs, jobFailureRetries, jobExecutor);

    }

    /**
     * Map to track the number of times a job has failed. A message id is
     * constant for the life of a message (whereas receipt handle changes each
     * time the message is re-driven from the queue when polling it), so we use
     * message id to identify a job. If a job fails jobFailureRetries times,
     * then it is deleted from the backlog and put it in the dead letter queue.
     */
    private Map<String, Integer> failedJobCounts = new HashMap<String, Integer>();

    /** Queue from which jobs are drawn */
    private MessageQueue backlogQueue;
    /** Queue into which job execution results are enqueued */
    private MessageQueue resultsQueue;
    /**
     * Queue into which jobs that have failed jobFailureRetries times are
     * enqueued
     */
    private MessageQueue deadLetterQueue;
    /** Number of times per minute to poll the backlog queue for jobs to execute */
    private double pollRatePerMin;
    /**
     * Amount of time each job is expected to take to execute. This is used to
     * set the visibility timeout when receiving messages.
     */
    private int expectedJobExecutionTimeInSecs;
    /**
     * Number of times a job is allowed to fail before being put in the dead
     * letter queue. In other words, it's the number of retries.
     */
    private int jobFailureRetries;
    /** Handles execution of an individual job given its definition */
    private JobExecutor jobExecutor;

    /**
     * Validates the input and constructs a QueueListener.
     * 
     * @param backlogQueue
     *            queue from which to pull job definitions
     * @param resultsQueue
     *            queue into which results go
     * @param deadLetterQueue
     *            queue into which repeatedly failed jobs go
     * @param pollRatePerMin
     *            number of times to poll the backlog per minute
     * @param expectedJobExecutionTimeInSecs
     *            amount of time (in seconds) each job is expected to take
     * @param jobFailureRetries
     *            number of times to retry a failed job
     * @param jobExecutor
     *            the object that will execute each job given its definition
     * @return a QueueListener object
     */
    public QueueListener(MessageQueue backlogQueue, MessageQueue resultsQueue,
            MessageQueue deadLetterQueue, double pollRatePerMin,
            int expectedJobExecutionTimeInSecs, int jobFailureRetries, JobExecutor jobExecutor) {
        if (backlogQueue == null) {
            throw new IllegalArgumentException("Backlog MessageQueue must not be null");
        }
        if (resultsQueue == null) {
            throw new IllegalArgumentException("Results MessageQueue must not be null");
        }
        if (deadLetterQueue == null) {
            throw new IllegalArgumentException("Dead letter MessageQueue must not be null");
        }
        if (pollRatePerMin <= 0.0) {
            throw new IllegalArgumentException("Poll rate per minute must be > 0");
        }
        if (expectedJobExecutionTimeInSecs <= 0) {
            throw new IllegalArgumentException("Expected job execution time must be > 0");
        }
        if (jobFailureRetries < 0) {
            throw new IllegalArgumentException("Job failure retries must be >= 0");
        }
        this.backlogQueue = backlogQueue;
        this.resultsQueue = resultsQueue;
        this.deadLetterQueue = deadLetterQueue;
        this.pollRatePerMin = pollRatePerMin;
        this.expectedJobExecutionTimeInSecs = expectedJobExecutionTimeInSecs;
        this.jobExecutor = jobExecutor;
    }

    public void start() {
        /*
         * We want to give the JobExecutor ample time to process a message
         * before it's re-driven, so messages will be read with a visibility
         * timeout of twice the expected time necessary to process jobs.
         * Visibility timeout maxes out at 2 hours.
         */
        int visibilityTimeout = Math.min(2 * expectedJobExecutionTimeInSecs, 7200);

        log.info("Starting to monitor job backlog " + backlogQueue.getUrl());
        // Continually try to receive and process jobs
        try {
            while (true) {
                getAndProcessJob(visibilityTimeout);
                Thread.sleep((long) (60000 / pollRatePerMin));
            }
        } catch (InterruptedException e) {
            // Let the polling stop
            log.info("Polling interrupted");
        }
    }

    /**
     * This method attempts to receive a message representing a job definition
     * from the backlog and execute it. If successful, the definition is deleted
     * from the backlog and the result is put in the results queue.
     * 
     * @param visibilityTimeout
     *            time (in seconds) to lock messages when they are received
     */
    private void getAndProcessJob(int visibilityTimeout) {
        log.debug("Attempting to receive a message from " + backlogQueue.getUrl()
                + " with visibility timeout of " + visibilityTimeout + " seconds");
        try {
            // Receive a message and lock it for enough time to process
            Message msg = backlogQueue.receiveMessage(visibilityTimeout);
            if (msg != null) {
                long receiveTime = System.currentTimeMillis();
                String jobDefinition = msg.getMessageBody();
                log.info("Received job definition: '" + jobDefinition + "', message id = '"
                        + msg.getMessageId() + "'");
                String result = jobExecutor.execute(jobDefinition, msg.getMessageId());
				log.info("Enqueueing job result: '" + result + "'");
				deleteAndEnqueueMessages(backlogQueue, msg, resultsQueue, result);
				// check whether the job execution took longer than the
				// visibility timeout
				long finishTime = System.currentTimeMillis();
				long processingTime = (finishTime - receiveTime) / 1000;
				if (visibilityTimeout < processingTime) {
				    log.warn("Message was locked with a visibility timeout of "
				            + visibilityTimeout + " seconds, but processing took "
				            + processingTime + " seconds, which means the message "
				            + "could have been re-driven to another host processing "
				            + "jobs on this queue before it was deleted. Consider "
				            + "increasing your visibility timeout.");
				}
            }
        } catch (SQSException se) {
            log.error("Error contacting SQS", se);
        }
    }

    /**
     * Handles failures from job execution.
     * 
     * If a job has failed jobFailureRetries times, it is removed from the
     * backlog and placed in the dead letter queue.
     * 
     * @param msg
     *            message that caused the failure
     * @param jobExecutionException
     *            details of the failure
     */
    @SuppressWarnings("unused")
	private void handleJobFailure(Message msg, JobExecutionException jobExecutionException) {
        log.error("Error executing job", jobExecutionException);
        Integer failCount = failedJobCounts.get(msg.getMessageId());
        if (failCount == null) {
            failCount = 0;
        }
        failCount++;
        log.error("Failure count for meassage id '" + msg.getMessageId() + "' = " + failCount);
        if (failCount < jobFailureRetries) {
            failedJobCounts.put(msg.getMessageId(), failCount);
        } else {
            log.error("Job failure count = " + failCount
                    + ", so removing from backlog and putting in dead letter queue");
            if (deleteAndEnqueueMessages(backlogQueue, msg, deadLetterQueue, msg.getMessageBody())) {
                failedJobCounts.remove(msg.getMessageId());
            } else {
                // failed to exchange messages, so don't lose failed job count
                failedJobCounts.put(msg.getMessageId(), failCount);
            }
        }
    }

    /**
     * Attempts to "atomically" enqueue a message in one queue and delete a
     * message from another. Atomically is in quotation marks, because no
     * rollback is attempted if the second operation fails.
     * 
     * @param fromQueue
     *            queue from which to remove message
     * @param messageToDelete
     *            message to delete from fromQueue
     * @param toQueue
     *            queue into which to send new message
     * @param messageToEnqueue
     *            message to send to toQueue
     * @return true if both operations are successful; false, otherwise
     */
    private boolean deleteAndEnqueueMessages(MessageQueue fromQueue, Message messageToDelete,
            MessageQueue toQueue, String messageToEnqueue) {
        try {
            log.debug("Sending '" + messageToEnqueue + "' to " + toQueue.getUrl());
            toQueue.sendMessage(messageToEnqueue);
        } catch (SQSException ex) {
            log.error("Failed to send message", ex);
            return false;
        }
        try {
            log.debug("Deleting message '" + messageToDelete.getMessageBody() + "', id = '"
                    + messageToDelete.getMessageId() + "' from " + fromQueue.getUrl());
            fromQueue.deleteMessage(messageToDelete.getReceiptHandle());
        } catch (SQSException ex) {
            log.error("Failed to delete message", ex);
            return false;
        }
        return true;
    }
}
