package csvsorter;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * this program sorts a csv input file using external sorting k-way merge sort algorithm.
 */
public class Main {
    private static final Logger logger = java.util.logging.Logger.getLogger(String.valueOf(Main.class));

    /**
     * Main method parses cmd args, then starts a timer.
     * and then creates the Mapper, and call it's map method.
     * afterwards creates the Reducer and call it's recursive reduce method.
     * then it handles the final output and cleans the temporary files if requested.
     * finally, it stops the timer and logs the time that it took to sort the file.
     *
     * @param args input arguments
     * @throws IOException if there is a problem with interaction with files
     *                     usage: csv-sorter
     *                     -in,--input arg     input file path
     *                     -key,--key-ind arg   sorting key index - the index of the field to sort by
     *                     -max,--max-rec arg   maximum number of records in memory
     *                     -out,--output arg   output path (optional)
     */
    public static void main(String[] args) throws IOException {
        //parse cmd args
        CommandLine cmd = getCommandOptions(args);
        String inputCSVFile = cmd.getOptionValue("input");
        String sortingKeyIndexStr = cmd.getOptionValue("keyind");
        String maxRecordsNumberStr = cmd.getOptionValue("maxrec");
        Properties appProperties = loadAppProperties();
        String tempFilesDir = getTempFilesDirFromAppProperties(appProperties);
        String outputFilePathOptionalArgOrDefault = cmd.hasOption("output") ? cmd.getOptionValue("output") : getOutputFilePathFromAppProperties(appProperties);

        //start timer to measure the total time it took to sort the input file
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        //map - divide the input file to smaller files the size of maxRecordsNumber and sort each part
        Mapper mapper = new Mapper(Integer.valueOf(sortingKeyIndexStr), Integer.valueOf(maxRecordsNumberStr));
        mapper.map(inputCSVFile, tempFilesDir);

        //reduce - sort the parts using external merge sort
        Reducer reducer = new Reducer(Integer.valueOf(sortingKeyIndexStr));
        String sortedOutputFilePath = reducer.reduceRec(tempFilesDir, Constants.MAP_SUFFIX, 1);

        //handle the final output and clean temporary files according to input args or default configuration
        moveSortedOutputFileToGivenLocation(outputFilePathOptionalArgOrDefault, sortedOutputFilePath);
        deleteTempDirWhenDone(appProperties, tempFilesDir);

        //done, stop the timer
        stopWatch.stop();
        logger.info("Sorting was completed for the file " + inputCSVFile +
                ". Time elapsed in seconds is: " + (double) stopWatch.getTime(TimeUnit.MILLISECONDS) / 1000);
    }

    private static void moveSortedOutputFileToGivenLocation(String outputFilePathOptionalArg, String sortedOutputFilePath) throws IOException {
        File outputFile = new File(outputFilePathOptionalArg);
        if (outputFile.exists()) {
            FileUtils.delete(outputFile);
        }
        FileUtils.moveFile(new File(sortedOutputFilePath), new File(outputFilePathOptionalArg));
    }

    private static void deleteTempDirWhenDone(Properties appProperties, String tempFilesDir) throws IOException {

        String cleanTempDirProperty = appProperties.getProperty(Constants.CLEAN_TEMP_DIR_PROPERTY);
        if (cleanTempDirProperty.equalsIgnoreCase("yes") ||
                cleanTempDirProperty.equalsIgnoreCase("y")) {
            FileUtils.deleteDirectory(new File(tempFilesDir));
        }
    }

    private static String getOutputFilePathFromAppProperties(Properties appProperties) {
        return appProperties.getProperty(Constants.DEFAULT_OUTPUT_FILE_PATH_PROPERTY);
    }

    private static String getTempFilesDirFromAppProperties(Properties appProperties) throws IOException {
        String projectBuildDir = appProperties.getProperty(Constants.PROJECT_BUILD_DIR) + "\\";
        String tempFilesDirPath = projectBuildDir + appProperties.getProperty(Constants.TEMP_FILES_DIR_NAME_PROPERTY) + "\\";
        //create the directory if it doesn't exist or clean it
        File tempdir = new File(tempFilesDirPath);
        FileUtils.forceMkdir(tempdir);
        FileUtils.cleanDirectory(tempdir);
        return tempFilesDirPath;
    }

    private static Properties loadAppProperties() throws IOException {
        URL resource = Thread.currentThread().getContextClassLoader().getResource("");
        String appConfigPath;
        if(resource == null) {
            appConfigPath = Constants.APP_PROPERTIES_FILE_NAME;
        }
        else{
            String rootPath = resource.getPath();
            appConfigPath = rootPath + Constants.APP_PROPERTIES_FILE_NAME;
        }
        Properties appProps = new Properties();
        appProps.load(new FileInputStream(appConfigPath));

        return appProps;
    }


    private static CommandLine getCommandOptions(String[] args) {
        Options options = new Options();

        Option input = new Option("in", "input", true, "input file path");
        input.setRequired(true);
        options.addOption(input);

        Option keyIndex = new Option("key", "keyind", true, "sorting key index - the index of the field to sort by");
        keyIndex.setRequired(true);
        options.addOption(keyIndex);

        Option maxRec = new Option("max", "maxrec", true, "maximum number of records in memory");
        maxRec.setRequired(true);
        options.addOption(maxRec);

        Option output = new Option("out", "output", true, "output path (optional)");
        output.setOptionalArg(true);
        options.addOption(output);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            logger.severe(e.getMessage());
            formatter.printHelp("csvsorter", options);
            System.exit(1);
        }
        return cmd;
    }
}
