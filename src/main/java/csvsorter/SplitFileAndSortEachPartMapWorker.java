package csvsorter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * this class will take a file that was created by the Mapper, and sort this file in memory.
 * this file contains maxNumberOfRecords records (or less)
 */
class SplitFileAndSortEachPartMapWorker extends MapReduceWorkersBase {
    private static final Logger logger = Logger.getLogger(String.valueOf(SplitFileAndSortEachPartMapWorker.class));

    String partFileName, inputCSVFile;
    int indexOfFirstLine, maxNumberOfRecords;

    /**
     * @param partFileName       - the name of the smaller file that is created for each part
     * @param indexOfFirstLine   - when dividing the large file into parts, this will indicate where to start the next part.
     * @param maxNumberOfRecords - maximum number of records that are allowed to be held in memory at a time.
     * @param inputCSVFile       - the path to the input csv file
     * @param sortingKeyIndex    - the index in a csv record (one record from inputCSVFile) of the key to sort by
     */
    SplitFileAndSortEachPartMapWorker(String partFileName, int indexOfFirstLine, int maxNumberOfRecords, String inputCSVFile, int sortingKeyIndex) {
        this.partFileName = partFileName;
        this.indexOfFirstLine = indexOfFirstLine;
        this.maxNumberOfRecords = maxNumberOfRecords;
        this.sortingKeyIndex = sortingKeyIndex;
        this.inputCSVFile = inputCSVFile;
    }

    /**
     * stream the lines of inputCSVFile, skip to the correct index, and create a list of lines the size of maxNumberOfRecords.
     * sort the list using merge sort algorithm according to the sorting key at sortingKeyIndex.
     * then open for writing partFileName, and write the sorted list to the file
     */
    public void run() {
        try {
            logger.info(Thread.currentThread().getName() + " is starting to write file " + partFileName);
            List<String> part =
                    Files.lines(Paths.get(inputCSVFile))
                            .skip(indexOfFirstLine - 1)
                            .limit(maxNumberOfRecords)
                            .collect(Collectors.toList());

            List<String> sortedPart = mergeSortPart(new ArrayList<>(part));

            BufferedWriter writer = Files.newBufferedWriter(Paths.get(partFileName));
            sortedPart.forEach(line -> writeToFile(writer, line));

            logger.info(Thread.currentThread().getName() + " finished writing file " + partFileName);
            writer.close();

        } catch (IOException e) {
            logger.severe(String.valueOf(e));
        }
    }

    @SuppressWarnings("ConstantConditions")
    private ArrayList<String> mergeSortPart(ArrayList<String> part) {
        ArrayList<String> left = new ArrayList<>();
        ArrayList<String> right = new ArrayList<>();
        int middle;

        if (part.size() == 1) {
            return part;
        } else {
            middle = part.size() / 2;
            for (int i = 0; i < middle; i++) {
                left.add(part.get(i));
            }

            for (int i = middle; i < part.size(); i++) {
                right.add(part.get(i));
            }

            left = mergeSortPart(left);
            right = mergeSortPart(right);

            merge(left, right, part);
        }
        return part;
    }

    private void merge(ArrayList<String> left, ArrayList<String> right, ArrayList<String> part) {
        int leftIndex = 0;
        int rightIndex = 0;
        int partIndex = 0;

        while (leftIndex < left.size() && rightIndex < right.size()) {
            if ((getSortingKey(left.get(leftIndex)).compareTo(getSortingKey(right.get(rightIndex)))) < 0) {
                part.set(partIndex, left.get(leftIndex));
                leftIndex++;
            } else {
                part.set(partIndex, right.get(rightIndex));
                rightIndex++;
            }
            partIndex++;
        }

        ArrayList<String> remainingPart;
        int remainingPartIndex;
        if (leftIndex >= left.size()) {
            remainingPart = right;
            remainingPartIndex = rightIndex;
        } else {
            remainingPart = left;
            remainingPartIndex = leftIndex;
        }

        for (int i = remainingPartIndex; i < remainingPart.size(); i++) {
            part.set(partIndex, remainingPart.get(i));
            partIndex++;
        }
    }

    private void writeToFile(BufferedWriter writer, String line) {
        try {
            writer.write(line + "\r\n");
        } catch (IOException e) {
            logger.severe(String.valueOf(e));
            throw new RuntimeException(e);
        }
    }
}