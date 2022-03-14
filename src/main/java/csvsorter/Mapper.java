package csvsorter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * this class will calculate the number of parts that the input file will be divided to,
 * and initiate worker threads to handle(sort) each part
 */
public class Mapper {
    private static final Logger logger = Logger.getLogger(String.valueOf(Mapper.class));

    private final int maxRecordsNumber;
    private final int sortingKeyIndex;

    /**
     * @param sortingKeyIndex  - the index of the key in the csv record, the sorting is done by to this key
     * @param maxRecordsNumber - maximum number of records that are allowed to be held in memory at a time.
     *                         each part (file) will contain maxRecordsNumber records or less,
     *                         this allows sorting of each part to be done in memory
     */
    public Mapper(int sortingKeyIndex, int maxRecordsNumber) {
        this.sortingKeyIndex = sortingKeyIndex;
        this.maxRecordsNumber = maxRecordsNumber;
    }

    /**
     * calculate the number of parts (by counting total number of lines and dividing by maxRecordsNumber)
     * then initiating a new thread pool of worker threads to handle the parts, and waiting for them to finish the work.
     *
     * @param inputCSVFile - path to input file
     * @param tempFilesDir - path to where all the smaller files (parts) will be written to
     */
    void map(String inputCSVFile, String tempFilesDir) throws IOException {
        long numberOfParts = countIInitialParts(inputCSVFile, maxRecordsNumber);
        ExecutorService taskExecutor = Executors.newCachedThreadPool();

        for (int i = 1; i <= numberOfParts; i++) {
            int startLine = i == 1 ? i : (i - 1) * maxRecordsNumber + 1;
            String partitionFileName = createMapFileName(i, tempFilesDir);
            SplitFileAndSortEachPartMapWorker mapThread =
                    new SplitFileAndSortEachPartMapWorker(partitionFileName, startLine, maxRecordsNumber, inputCSVFile, sortingKeyIndex);
            taskExecutor.execute(mapThread);
        }
        taskExecutor.shutdown();
        try {
            taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);//let's wait till the end of time
        } catch (InterruptedException e) {
            logger.severe(e.getMessage());
        }
    }

    private long countIInitialParts(String inputCSVFile, int maxRecordsNumber) throws IOException {
        long inputCSVFileLinesCounter = Files.lines(Paths.get(inputCSVFile)).count();;
        Files.lines(Paths.get(inputCSVFile)).count();
        if (inputCSVFileLinesCounter % maxRecordsNumber != 0)
            return (inputCSVFileLinesCounter / maxRecordsNumber) + 1;
        else
            return inputCSVFileLinesCounter / maxRecordsNumber;
    }

    private String createMapFileName(int partNumber, String tempFilesDir) {
        return tempFilesDir + Constants.PART_PREFIX + partNumber + Constants.MAP_SUFFIX;
    }
}
