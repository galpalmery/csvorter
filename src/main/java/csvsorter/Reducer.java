package csvsorter;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * this class scans the "tempFilesDir" to find the files that will be merged in this merge iteration,
 * initiates worker threads to handle the merge, and then calls recursively until there is only 1 file left in the iteration
 */
public class Reducer {

    private static final Logger logger = Logger.getLogger(String.valueOf(Reducer.class));

    private final int sortingKeyIndex;

    /**
     * @param sortingKeyIndex - the index of the key in the csv record, the sorting is done by to this key
     */
    public Reducer(int sortingKeyIndex) {
        this.sortingKeyIndex = sortingKeyIndex;
    }

    /**
     * this method will scan the tempFilesDir and find all the files with filename that end with fileSuffix.
     * these are the files that will be handled in the current iteration.
     * then initiating a new thread pool of worker threads to handle the merges, and waiting for them to finish the work.
     * the workers will merge each 2 files to 1 file, and in case of odd number of files, the last file will be renamed,
     * to be merged in the next iteration.
     *
     * @param tempFilesDir - the path on the disc to all the parts of the file that were created by the mapper,
     *                     and also where this method will create the files during the recursive merge iterations.
     * @param fileSuffix   - the suffix that is concatenated to the name of the file that is created in each merger of 2 files
     * @param reduceDepth  - indicates the number of iteration in the recursion,
     *                     effects the naming of the files that are created in each iteration,
     *                     so that when the next iteration will scan the folder it'll find the relevant files.
     * @return - the path to the final sorted file
     * @throws IOException - in case of any IO error
     */
    String reduceRec(String tempFilesDir, String fileSuffix, int reduceDepth) throws IOException {

        List<File> files = findFilesForThisIteration(tempFilesDir, fileSuffix);
        if (files.size() == 1) {
            return files.get(0).getPath();
        }

        ExecutorService taskExecutor = Executors.newCachedThreadPool();

        for (int i = 0, j = 1; i < files.size(); i += 2, j++) {
            String mergeResultFileName = createMergeResultFileName(tempFilesDir, reduceDepth, j);
            String tempFileName1 = files.get(i).getPath();
            //if tempFileName1 is the last file in this reduce iteration, then rename it so that it will be merged in the next iteration
            if (i + 1 == files.size()) {
                File mergeResultFile = new File(mergeResultFileName);
                FileUtils.moveFile(files.get(i), mergeResultFile);
                break;
            }
            String tempFileName2 = files.get(i + 1).getPath();

            MergeSortedPartsReduceWorker reduceThread =
                    new MergeSortedPartsReduceWorker(tempFileName1, tempFileName2, mergeResultFileName, sortingKeyIndex);
            taskExecutor.execute(reduceThread);
        }
        taskExecutor.shutdown();
        try {
            taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);//let's wait till the end of time
        } catch (InterruptedException e) {
            logger.severe(String.valueOf(e));
        }

        return reduceRec(tempFilesDir, Constants.REDUCE_SUFFIX + reduceDepth, reduceDepth + 1);
    }

    private List<File> findFilesForThisIteration(String tempFilesDir, String fileSuffix) throws IOException {
        return Files.list(Paths.get(tempFilesDir))
                .map(Path::toFile)
                .filter(file -> file.getName().endsWith(fileSuffix))
                .collect(Collectors.toList());
    }

    private String createMergeResultFileName(String tempFilesDir, int reduceDepth, int partNumber) {
        return tempFilesDir + Constants.PART_PREFIX + partNumber + Constants.REDUCE_SUFFIX + reduceDepth;
    }
}
