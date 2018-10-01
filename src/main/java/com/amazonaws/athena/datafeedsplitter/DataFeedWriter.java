package com.amazonaws.athena.datafeedsplitter;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static com.amazonaws.athena.datafeedsplitter.LambdaHandler.BUFFER;
import static com.amazonaws.athena.datafeedsplitter.LambdaHandler.PART_MINIMUM;

/**
 * DataFeedWriter is a wrapper for the logic that writes individual files of the Data Feed file to S3.
 * It is responsible for determining if the files can be uploaded in one shot, or if we need to switch
 * to a multi-part upload in order to extract the files without writing to disk.
 *
 * It is also responsible for determining if the lookup files need to be copied to "latest lookups" location,
 * as well as performing the copy.
 */
class DataFeedWriter {
    private TransferManager tm;
    private List<Copy> copyResults;
    private List<PartETag> partETags;
    private LambdaLogger logger;
    private AmazonS3 s3;
    private DataFeedRecord df;

    static private final int MULTIPART_THRESHOLD = PART_MINIMUM * 4;

    public DataFeedWriter(final LambdaLogger logger, final AmazonS3 s3, final DataFeedRecord df) {
        /*
        DataFeedWriter uses an in-memory byte array in order to transfer files to S3 without writing intermediary
        data out to disk. This is primarily due to the 500MB disk limit in AWS Lambda.

        Files are written in Gzip format and certain files are duplicated as they are "lookup" files.
        */
        tm = TransferManagerBuilder.standard().withS3Client(s3).build();
        copyResults = new ArrayList<Copy>();
        partETags = new ArrayList<PartETag>();
        this.logger = logger;
        this.s3 = s3;
        this.df = df;
    }

    public void writeEntry(final DataFeedReader reader, final TarArchiveEntry entry) throws IOException {
        // Flag and instance for multi-part upload requests
        boolean multiPartRequired = false;
        InitiateMultipartUploadResult multipartUpload = null;

        byte[] data = new byte[BUFFER];
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        GZIPOutputStream outputStream = new GZIPOutputStream(byteOut);
        int readCount;

        // For easy reference, create local variables for our destination bucket and key
        String dstBucket = df.getDstBucket();
        String dstKey = df.getDstKeyForBasename(entry.getName());

        // Read all bytes from the reader
        while ((readCount = reader.read(data, 0, BUFFER)) != -1) {
            outputStream.write(data, 0, readCount);

            // If we reach the threshold of multi-part uploads (+ some buffer), create a new instance
            if (!multiPartRequired && byteOut.size() > MULTIPART_THRESHOLD) {
                logger.log("  " + entry.getName() + " - enabling multi-part upload");
                multiPartRequired = true;
                multipartUpload = s3.initiateMultipartUpload(new InitiateMultipartUploadRequest(dstBucket, dstKey));
            }

            // If we're multi-part and we need to upload some data, do so
            if (multiPartRequired && byteOut.size() > MULTIPART_THRESHOLD) {
                uploadPart(dstKey, byteOut, multipartUpload.getUploadId());
            }
        }

        // Close our output stream and either write the entire buffer (if small enough) or the last multi-part upload.
        outputStream.close();
        if (multiPartRequired) {
            uploadPart(dstKey, byteOut, multipartUpload.getUploadId());
            logger.log("  " + entry.getName() + " - completing multi-part upload with " + partETags.size() + " parts");
            CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(
                    dstBucket,
                    dstKey,
                    multipartUpload.getUploadId(),
                    partETags
            );
            s3.completeMultipartUpload(compRequest);
        } else {
            oneShot(byteOut, dstKey);
        }

        copyIfLookupFile(entry.getName());
    }

    public void waitForAllCopyResults() throws InterruptedException {
        for (Copy copyResult : copyResults) {
            copyResult.waitForCopyResult();
        }
    }

    private void copyIfLookupFile(final String basename) {
        if (!basename.equals("hit_data.tsv") && !basename.equals("column_headers.tsv")) {
            Copy copy = tm.copy(
                    df.getDstBucket(),
                    df.getDstKeyForBasename(basename),
                    df.getDstBucket(),
                    df.getLookupKeyForBasename(basename)
            );
            copyResults.add(copy);
        }
    }

    /**
     * @param dstKey The S3 key of the file to upload to
     * @param outputStream A stream of bytes to be written to the uploader
     * @param uploadId The previously generated ID of the Multipart Request
     *
     * uploadPart uploads a chunk of an S3 multipart upload.
     * In addition, it will also reset the ByteArrayOutputStream that is passed to to it.
     */
    private void uploadPart(final String dstKey, final ByteArrayOutputStream outputStream, final String uploadId) {
        long partSize = outputStream.size();
        int partNumber = partETags.size() + 1;

        logger.log("  (" + partNumber + ") - " + dstKey);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        UploadPartRequest uploadPartRequest = new UploadPartRequest()
                .withBucketName(df.getDstBucket())
                .withKey(dstKey)
                .withPartNumber(partNumber)
                .withUploadId(uploadId)
                .withInputStream(inputStream)
                .withPartSize(partSize);
        UploadPartResult uploadPartResult = s3.uploadPart(uploadPartRequest);
        partETags.add(uploadPartResult.getPartETag());
        outputStream.reset();
    }

    private void oneShot(final ByteArrayOutputStream byteOut, final String dstKey) {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(byteOut.toByteArray());
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(byteOut.size());
        logger.log(" - uploading to " + dstKey);
        s3.putObject(new PutObjectRequest(df.getDstBucket(), dstKey, inputStream, metadata));
    }
}
