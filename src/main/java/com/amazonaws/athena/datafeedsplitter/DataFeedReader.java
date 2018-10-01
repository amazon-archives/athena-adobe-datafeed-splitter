package com.amazonaws.athena.datafeedsplitter;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import java.io.IOException;
import java.util.zip.GZIPInputStream;

import static com.amazonaws.athena.datafeedsplitter.LambdaHandler.BUFFER;

/**
 * DataFeedReader is a wrapper for the logic that can read Adobe Analytics Data Feed files.
 * These files are .tar.gz archives that can be up to 2GB in size.
 */
class DataFeedReader {
    private TarArchiveInputStream tarReader;

    /**
     * Creates a new DataFeedReader class that can read from the provided AmazonS3 and DataFeedRecord instances.
     *
     * @param s3    A pre-existing AmazonS3 client
     * @param record    A DataFeedRecord that indicates the source archive to read from
     * @throws RuntimeException A generic exception if anything goes wrong while initializing
     */
    public DataFeedReader(final AmazonS3 s3, final DataFeedRecord record) throws RuntimeException {
        // Initialize an S3 Object stream
        S3ObjectInputStream s3is = s3.getObject(
                record.getSrcBucket(),
                record.getSrcTarbell()
        ).getObjectContent();

        GZIPInputStream gzIn;
        try {
            gzIn = new GZIPInputStream(s3is, BUFFER);
        } catch (IOException e) {
            throw new RuntimeException("Could not open GZIPInputStream", e);
        }

        tarReader = new TarArchiveInputStream(gzIn);
    }

    public TarArchiveEntry getNextEntry() throws IOException {
        return (TarArchiveEntry) tarReader.getNextEntry();
    }

    public int read(final byte[] buf, final int offset, final int numToRead) throws IOException {
        return tarReader.read(buf, offset, numToRead);
    }

    public void close() throws IOException {
        tarReader.close();
    }
}
