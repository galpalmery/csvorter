package csvsorter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * this class will merge 2 input files (inputFile1 and inputFile2) into 1 merged file (mergedFile),
 * according to the sorting key (at sortingKeyIndex).
 * The merge is done by external sorting (without loading the files to memory).
 */
class MergeSortedPartsReduceWorker extends MapReduceWorkersBase {
    private static final Logger logger = Logger.getLogger(String.valueOf(MergeSortedPartsReduceWorker.class));

    String inputFile1, inputFile2, mergedFile;

    /**
     * @param inputFile1 - a file that contains a part of the (larger) csv file
     * @param inputFile2 - same as above
     * @param mergedFile - the file that will contain the merged result of inputFile1 and inputFile2
     * @param sortingKeyIndex - member of MapReduceWorkersBase, the index in the csv record of the key to sort by
     */
    MergeSortedPartsReduceWorker(String inputFile1, String inputFile2, String mergedFile, int sortingKeyIndex) {
        this.inputFile1 = inputFile1;
        this.inputFile2 = inputFile2;
        this.mergedFile = mergedFile;
        this.sortingKeyIndex = sortingKeyIndex;
    }

    /**
     * open for read the 2 input files, open for write the mergedFile.
     * read line by line, and compare the lines from each file by the sorting key,
     * then write the matching line to the mergedFile
     */
    public void run() {
        try {
            logger.fine("Started Merging " + inputFile1 + " with " + inputFile2);
            FileReader fileReader1 = new FileReader(inputFile1);
            BufferedReader bufferedReader1 = new BufferedReader(fileReader1);
            FileReader fileReader2 = new FileReader(inputFile2);
            BufferedReader bufferedReader2 = new BufferedReader(fileReader2);

            FileWriter writer = new FileWriter(mergedFile);

            String lineFromFirstFile = bufferedReader1.readLine();
            String lineFromSecondFile = bufferedReader2.readLine();
            while (lineFromFirstFile != null || lineFromSecondFile != null) {
                if (lineFromFirstFile == null || (lineFromSecondFile != null
                        && (getSortingKey(lineFromFirstFile)).compareTo(getSortingKey(lineFromSecondFile)) > 0)) {
                    writer.write(lineFromSecondFile + "\r\n");
                    lineFromSecondFile = bufferedReader2.readLine();
                } else {
                    writer.write(lineFromFirstFile + "\r\n");
                    lineFromFirstFile = bufferedReader1.readLine();
                }
            }
            writer.close();
            fileReader1.close();
            fileReader2.close();
            logger.info("Done Merging " + inputFile1 + " and " + inputFile2);
        } catch (IOException e) {
            logger.severe(String.valueOf(e));
        }
    }
}