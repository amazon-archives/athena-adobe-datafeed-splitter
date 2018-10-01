package com.amazonaws.athena.datafeedsplitter;


import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;

import java.io.IOException;

/**
 * LambdaHandler implements a Lambda function triggered on S3 Create events that accepts an Adobe Analytics Data Feed file.
 * If the file received is a metadata file, that is an indication that all data files have been uploaded.
 * This job will take a data file and extract it's contents to Athena-friendly S3 locations.
 */
public class LambdaHandler implements RequestHandler<S3Event, String>  {
    // Read 50MB at a time
    static final int BUFFER = 50 * 1024 * 1024;

    // Minimum part size to use the S3 multi-part upload API
    static final int PART_MINIMUM = 5 * 1024 * 1024;

    /**
     * @param input The S3 event associated with this Lambda job
     * @param context Lambda context
     * @return a value for the result of this job. Currently only "OK" or "SKIP" if we do not process the file.
     */
    @Override
    public String handleRequest(final S3Event input, final Context context) {
        LambdaLogger logger = context.getLogger();
        S3EventNotification.S3EventNotificationRecord record = input.getRecords().get(0);

        DataFeedSplitterManager manager = new DataFeedSplitterManager(logger, record);

        if (!manager.shouldProcessFile()) {
            logger.log("Received record was not a metadata file, ignoring.");
            return "SKIP";
        }

        try {
            manager.processAllEntries();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error reading from Data Feed archive", e);
        }

        manager.triggerCatalogManager();

        return "OK";
    }
}
