package csvsorter;

import com.opencsv.CSVParser;

import java.io.IOException;

/**
 * this is a base class for the map and reduce workers, that has the part that both of the workers need
 */
class MapReduceWorkersBase extends Thread {

    int sortingKeyIndex;

    /**
     * this method handles csv parsing for a single record. both mapper and reducer use this while sorting/
     *
     * @param record - a string that represents a single csv record
     * @return the key at sortingKeyIndex from the given record
     */
    public String getSortingKey(String record) {
        CSVParser parser = new CSVParser();
        String sortingKey = null;
        try {
            String[] fields = parser.parseLine(record);
            sortingKey = fields[sortingKeyIndex];
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sortingKey;
    }
}