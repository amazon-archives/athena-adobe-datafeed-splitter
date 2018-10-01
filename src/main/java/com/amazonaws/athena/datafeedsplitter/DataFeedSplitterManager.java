package com.amazonaws.athena.datafeedsplitter;

import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.invoke.LambdaInvokerFactory;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;

import java.io.IOException;

/**
 * DataFeedSplitterManager manages the reading of Data Feed entries from the input Tar file and writing them
 * out to individual S3 gzip files in a format that is Athena-friendly.
 */
class DataFeedSplitterManager {
    private LambdaLogger logger;
    private DataFeedRecord dfRecord;
    private DataFeedReader reader;
    private DataFeedWriter writer;

    /**
     * @param logger The Lambda logger sent in to the parent job
     * @param record The S3 event notification record
     */
    public DataFeedSplitterManager(final LambdaLogger logger,
                                   final S3EventNotification.S3EventNotificationRecord record) {
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        this.logger = logger;

        dfRecord = DataFeedRecord.newFromRecord(record);
        reader = new DataFeedReader(s3, dfRecord);
        writer = new DataFeedWriter(logger, s3, dfRecord);
    }

    /**
     * @return a value indicating whether we should process this file or not.
     *
     * Slightly confusing, we do not allow processing of the actual .tar.gz files, but rather when we receive
     * a .txt file indicating that the .tar.gz upload is complete.
     */
    public boolean shouldProcessFile() {
        return dfRecord.isMetadataFile();
    }

    /**
     * @throws IOException when there is an error reading from the .tar.gz file or writing to S3.
     */
    public void processAllEntries() throws IOException {
        TarArchiveEntry entry;

        while ((entry = reader.getNextEntry()) != null) {
            logger.log("Processing: " + entry.getName());
            writer.writeEntry(reader, entry);
        }

        // Close each of the reader and writer. Ignore any errors for now.
        try {
            reader.close();
        } catch (IOException e) {
            logger.log("Error closing tar archive, continuing...");
            e.printStackTrace();
        }

        try {
            writer.waitForAllCopyResults();
        } catch (InterruptedException e) {
            logger.log("Error waiting for copy, ignoring...");
            e.printStackTrace();
        }
    }

    /**
     * Trigger a dependent Lambda function that will add the data created by this job to the Glue data catalog.
     * If no database or tables exist, they will be created.
     * The "hit_data" table is partitioned and the data for this Data Feed's date will be added to the partitions.
     */
    public void triggerCatalogManager() {
        final CatalogManagerTrigger catManager = LambdaInvokerFactory.builder()
                .lambdaClient(AWSLambdaAsyncClientBuilder.defaultClient())
                .lambdaFunctionNameResolver(new EnvironmentLambdaFunctionNameResolver())
                .build(CatalogManagerTrigger.class);

        CatalogManagerInput jobInput = new CatalogManagerInput();
        jobInput.setReportBase(dfRecord.getReportBasedDestinationURI());
        jobInput.setLookupURI(dfRecord.getLatestLookupsURI());
        jobInput.setReportDate(dfRecord.getReportDate());

        logger.log("Triggering data catalog lambda function " + System.getenv("CATALOG_MANAGER_LAMBDA"));
        catManager.addParts(jobInput);
    }
}
