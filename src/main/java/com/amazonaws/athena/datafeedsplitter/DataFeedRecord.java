package com.amazonaws.athena.datafeedsplitter;

import com.amazonaws.services.s3.event.S3EventNotification;

/**
 * DataFeedRecord maintains the source and destination defaults for Data Feed files.
 * Data Feed reports get dropped in an S3 bucket with a certain prefix. This class
 * knows how to parse the name of the Data Feed file to determine the report suite name and date.
 *
 * More information about the Data Feed contents can be found on the Adobe Analytics site:
 * https://marketing.adobe.com/resources/help/en_US/reference/datafeeds_contents.html
 */
public class DataFeedRecord {
    private String srcBucket;
    private String srcFilename;
    private String reportName;
    private String reportDate;

    private String dstBucket;

    public String getReportDate() {
        return reportDate;
    }

    public String getSrcBucket() {
        return srcBucket;
    }

    public String getDstBucket() {
        return dstBucket;
    }

    /**
     * @return the S3 prefix where converted TSV files are stored.
     */
    public String getRawTSVPrefix() {
        return String.format("%s/%s/rawtsv", getConvertedPrefix(), reportName);
    }

    /**
     * @return the S3 prefix where the latest version of lookup files are stored.
     */
    public String getLatestLookupsPrefix() {
        return String.format("%s/%s/latest_lookups", getConvertedPrefix(), reportName);
    }

    /**
     * @param basename the base filename of the file to generate a path for
     * @return the full S3 prefix to a converted TSV file.
     */
    public String getDstKeyForBasename(final String basename) {
        final String tableName = basename.replace(".tsv", "");
        return String.format("%s/%s/%s/%s.gz", getRawTSVPrefix(), tableName, getDatePartition(), basename);
    }

    /**
     * @param basename the base filename of the file to generate a path for
     * @return the full S3 prefix to the latest version of a specific lookup file.
     */
    public String getLookupKeyForBasename(final String basename) {
        final String tableName = basename.replace(".tsv", "");
        return String.format("%s/%s/%s.gz", getLatestLookupsPrefix(), tableName, basename);
    }

    public String getSrcTarbell() {
        return srcFilename.replace(".txt", ".tar.gz");
    }

    public String getReportName() {
        return reportName;
    }

    /**
     * @return the prefix for converted Data Feed TSV files
     */
    public String getConvertedPrefix() {
        return "adobe/converted";
    }

    /**
     * @return the name of the date partition for this Data Feed
     */
    public String getDatePartition() {
        return String.format("dt=%s", reportDate);
    }

    /**
     * @return the S3 URI for the base path to converted data for this Data Feed
     */
    public String getReportBasedDestinationURI() {
        return String.format("s3://%s/%s/%s", dstBucket, getConvertedPrefix(), reportName);
    }

    /**
     * @return the S3 URI for the path to the latest version of lookup files for this Data Feed
     */
    public String getLatestLookupsURI() {
        return String.format("%s/latest_lookups", getReportBasedDestinationURI());
    }

    /**
     * @param record The S3 record associated with the Lambda event that triggered the splitter.
     */
    public void buildFromRecord(final S3EventNotification.S3EventNotificationRecord record) {
        // Extract our source information
        srcBucket = record.getS3().getBucket().getName();
        srcFilename = record.getS3().getObject().getKey();

        // Destination bucket is currently the same as the source bucket
        dstBucket = srcBucket;

        reportName = getReportNameFromSource(srcFilename);
        reportDate = getDateFromSource(srcFilename);
    }

    public boolean isMetadataFile() {
        return srcFilename.matches(".*\\.txt");
    }

    /*
    Extract the report name from the source filename
    Files are named according to the following; <report_suite_id>_YYYY_MM_DD
    */
    private String getReportNameFromSource(final String source) {
        String[] split = source.split("/");
        String filename = split[split.length - 1];
        return filename.split("_")[0];
    }

    private String getDateFromSource(final String source) {
        String[] split = source.split("/");
        String filename = split[split.length - 1];

        String filebase = filename.split("\\.")[0];

        split = filebase.split("_");

        return split[split.length - 1];
    }

    public static DataFeedRecord newFromRecord(final S3EventNotification.S3EventNotificationRecord record) {
        DataFeedRecord dfr = new DataFeedRecord();

        // Define our source bucket and filename
        dfr.buildFromRecord(record);

        return dfr;
    }

}
