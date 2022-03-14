package csvsorter;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@ExtendWith(MockitoExtension.class)
public class MapperTest {

    @Test
    public void testMapper_10records_3max() throws IOException {
        String inputCSVFile = "src\\test\\resources\\inputTest.csv";
        String tempFilesDir = "src\\test\\resources\\temp-test\\";
        File tempdir = new File(tempFilesDir);
        FileUtils.forceMkdir(tempdir);
        FileUtils.cleanDirectory(tempdir);

        Mapper mapper = new Mapper(2, 3);
        mapper.map(inputCSVFile, tempFilesDir);

        //input file contains 10 records, and the maxRecordsNumber that was sent to map was 3, so we need to have 4 parts.
        assert Files.list(Paths.get(tempFilesDir)).count() == 4;
    }

    @Test
    public void testMapper_10records_2max() throws IOException {
        String inputCSVFile = "src\\test\\resources\\inputTest.csv";
        String tempFilesDir = "src\\test\\resources\\temp-test\\";
        File tempdir = new File(tempFilesDir);
        FileUtils.forceMkdir(tempdir);
        FileUtils.cleanDirectory(tempdir);

        Mapper mapper = new Mapper(2, 2);
        mapper.map(inputCSVFile, tempFilesDir);

        //input file contains 10 records, and the maxRecordsNumber that was sent to map was 2, so we need to have 5 parts.
        assert Files.list(Paths.get(tempFilesDir)).count() == 5;
    }
}
