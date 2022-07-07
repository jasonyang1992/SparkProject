package Util;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class FileSystemUtil {

    public static Dataset<Row> readFileInput(final String fileName) throws Exception {
        return readFile(PropertyUtil.getProperty("File.Input.Location"), fileName);
    }

    public static Dataset<Row> readFileOutput(final String fileName) throws Exception {
        return readFile(PropertyUtil.getProperty("File.Output.Location"), fileName);
    }

    public static Dataset<Row> readFile(final String fileLocation, final String fileName) throws Exception {
        // Checks if the file exist or not
        if (Files.exists(Paths.get(fileLocation+fileName))) {
            return SparkUtil.getSparkSession().read()
                    .option("header", true)
                    .option("delimiter", "|")
                    .csv("/Users/jason/IdeaProjects/SparkBatchProject/dataProcess.dat");
        } else {
            throw new Exception("File does not exist for " + fileLocation + fileName);
        }
    }

    public static void writeFileInputLocation(final Dataset<Row> sourceDS, final String filename) throws IOException {
        writeToFile(sourceDS, PropertyUtil.getProperty("File.Input.Location"), filename);
    }

    public static void writeFileOutputLocation(final Dataset<Row> sourceDS, final String fileName) throws IOException {
        writeToFile(sourceDS, PropertyUtil.getProperty("File.Output.Location"), fileName);
    }

    public static void writeToFile(final Dataset<Row> sourceDS, final String fileLocation, final String fileName) throws IOException {
        // Check if file exist or not
        if(!Files.exists(Paths.get(fileLocation+fileName))){
            Files.createFile(Paths.get(fileLocation+fileName));
        }

        // saves the file
        sourceDS.repartition(1).write().mode(SaveMode.Overwrite)
                .option("header", "true")
                .option("delimiter", "|")
                .option("quote", "\u0000")
                .csv(fileLocation+fileName);

        // Creates the .dat file
        Stream<Path> pathStream = Files.list(Paths.get(fileLocation+fileName));
        {
            pathStream.filter(path -> path.toString().endsWith(".csv")).forEach(path -> {
                path.toFile().renameTo(new File(fileLocation+fileName+PropertyUtil.getProperty("File.Format")));
            });
        }
    }

    public static boolean validateColumns(Dataset<Row> sourceDS) {
        List<String> sourceList = Arrays.asList(sourceDS.columns());
        List<String> validateColumns = Arrays.asList("Name", "Age");
        return validateColumns.containsAll(sourceList);
    }

}
