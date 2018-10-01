package com.amazonaws.athena.datafeedsplitter;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import software.amazon.ion.IonException;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class DataFeedReaderTest {

    private final static String TEST_BUCKET = "test-delivery";
    private DataFeedReader dataFeedReader;
    private final DataFeedRecord dataFeedRecord = new DataFeedRecord();


    private byte[] readResource(String path) throws IOException {
        return Files.readAllBytes(Paths.get(this.getClass().getResource(path).getFile()));
    }

    @org.junit.Before
    public void setUp() throws Exception {
        AmazonS3 s3 = Mockito.mock(AmazonS3.class);
        S3Object mockS3Object = Mockito.mock(S3Object.class);

        Mockito.when(mockS3Object.getObjectContent()).thenReturn(
                new S3ObjectInputStream(new ByteArrayInputStream(readResource("/small_archive.tar.gz")), null)
        );

        // Return a sample entry when queried
        Mockito.when(s3.getObject(TEST_BUCKET, "adobe/daily/small_archive.tar.gz")).thenReturn(mockS3Object);


        // Set up the data feed record with a metadata event for the archive file
        final String sampleS3ArchiveEvent = new String(
                readResource("/s3_small_archive_event.json"),
                StandardCharsets.UTF_8
        );
        S3EventNotification notification = S3EventNotification.parseJson(sampleS3ArchiveEvent);
        dataFeedRecord.buildFromRecord(notification.getRecords().get(0));
        dataFeedReader = new DataFeedReader(s3, dataFeedRecord);
    }

    @org.junit.Test
    public void getNextEntry() throws IOException {
        // There should be two files in this archive
        TarArchiveEntry tarArchiveEntry = dataFeedReader.getNextEntry();
        assertEquals(tarArchiveEntry.getName(), "test1.txt");

        tarArchiveEntry = dataFeedReader.getNextEntry();
        assertEquals(tarArchiveEntry.getName(), "test2.txt");


        tarArchiveEntry = dataFeedReader.getNextEntry();
        assertNull(tarArchiveEntry);
    }

    @org.junit.Test
    public void read() throws IOException {
        final int BUFFER = 100000;
        byte[] data = new byte[BUFFER];
        int readCount;

        String test1Text = "Hello, World!\n" + "\n" + "  -- Damon 2.0\n";
        String test2Text = "Initiating boot sequence...\n";

        dataFeedReader.getNextEntry();
        readCount = dataFeedReader.read(data, 0, BUFFER);

        // Ensure we got some data
        assertTrue(readCount > 0);

        // Ensure the data is correct
        String fileString = new String(data, 0, readCount);
        assertEquals(test1Text, fileString);

        // Ensure another read gets us 0 bytes
        readCount = dataFeedReader.read(data, 0, BUFFER);
        assertEquals(-1, readCount);

        // Now try to get another entry
        dataFeedReader.getNextEntry();
        readCount = dataFeedReader.read(data, 0, BUFFER);
        assertTrue(readCount > 0);
        fileString = new String(data, 0, readCount);
        assertEquals(test2Text, fileString);
        assertEquals(-1, dataFeedReader.read(data, 0, BUFFER));
    }
}