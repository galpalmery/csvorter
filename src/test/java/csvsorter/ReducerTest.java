package csvsorter;

import com.google.common.collect.Ordering;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static java.nio.file.Files.lines;

public class ReducerTest {

    @Test
    public void testReducer_10records_3max() throws IOException {
        String tempFilesDir = "src\\test\\resources\\reducer-test-temp\\";
        int sortingKeyIndex = 0;

        Reducer reducer = new Reducer(sortingKeyIndex);
        String sortedOutputFilePath = reducer.reduceRec(tempFilesDir, Constants.MAP_SUFFIX, 1);

        List<String> lines = lines(Paths.get(sortedOutputFilePath)).collect(Collectors.toList());

        assert lines(Paths.get(sortedOutputFilePath)).count() == 10;
        assert Ordering.natural().isOrdered(lines);
    }
}
