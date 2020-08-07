package app;

import io.delta.tables.DeltaTable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class CSVTests
{
    private static Logger logger = LoggerFactory.getLogger(CSVTests.class);

    @Test
    public void testQueries() throws Exception
    {
        try {
            String location = "/tmp/IrisTable-"+ UUID.randomUUID().toString();

            SparkSession spark = SparkSession.builder()
                    .master("local")
                    .appName("CRUDApp")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .getOrCreate();

            System.out.println("INIT_DATA");
            final Dataset<Row> csv = spark.read().csv(Thread.currentThread().
                    getContextClassLoader().
                    getResource("iris.csv").getFile());
            csv.show();

            csv.write().format("delta").save(location);

            DeltaTable deltaTable = DeltaTable.forPath(location);
            deltaTable.toDF().show();
            //deltaTable.toDF().

            //RecordReader recordReader = new CSVRecordReader(numLinesToSkip,delimiter);

            spark.close();
        }
        catch(Exception e)
        {
            logger.error(e.getMessage(), e);
        }
    }
}
