package app;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.UUID;

public class CRUDAppTests
{
    private static Logger logger = LoggerFactory.getLogger(CRUDAppTests.class);

    @Test
    public void testQueries() throws Exception
    {
        try {
            String location = "/tmp/delta-table-"+ UUID.randomUUID().toString();

            SparkSession spark = SparkSession.builder()
                    .master("local")
                    .appName("CRUDApp")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .getOrCreate();

            //Initialize the Data
            System.out.println("INIT_DATA");
            Dataset<Long> data = spark.range(0, 5);
            data.write().format("delta").save(location);
            Dataset<Row> df = spark.read().format("delta").load(location);
            df.show();

            //Read the data back in Stream format
            //StreamingQuery stream = spark.readStream().format("delta").
            //        load(location).writeStream().
            //        format("console").start();
            //System.out.println(stream.toString());

            //Overwrite the data to see if the data got stored in a new data set
            System.out.println("OVERWRITE_DATA");
            data = spark.range(5, 10);
            //location = "/tmp/delta-table-"+ UUID.randomUUID().toString();
            data.write().format("delta").mode("overwrite").save(location);
            df = spark.read().format("delta").load(location);
            df.show();

            DeltaTable deltaTable = DeltaTable.forPath(location);

            // Update every even value by adding 100 to it
            System.out.println("UPDATE_DATA");
            deltaTable.update(
                    functions.expr("id % 2 == 0"),
                    new HashMap<String, Column>() {{
                        put("id", functions.expr("id + 100"));
                    }}
            );
            deltaTable.toDF().show();


            //Read in the old overwritten data set
            System.out.println("TIMETRAVEL_DATA_ORIGINAL");
            df = spark.read().format("delta").option("versionAsOf", 0).load(location);
            df.show();

            //Read in the updated data set
            System.out.println("TIMETRAVEL_DATA_UPDATED");
            df = spark.read().format("delta").option("versionAsOf", 1).load(location);
            df.show();

            //Read in the latest/live data set
            System.out.println("TIMETRAVEL_DATA_LATEST_LIVE");
            df = spark.read().format("delta").option("versionAsOf", 2).load(location);
            df.show();

            spark.close();
        }
        catch(Exception e)
        {
            logger.error(e.getMessage(), e);
        }
    }

    @Test
    public void testTableStreaming() throws Exception
    {
        String location = "/tmp/delta-table-"+ UUID.randomUUID().toString();

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("CRUDApp")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .getOrCreate();

        //Read the DataSet in a Stream format and write to another location in stream format
        Dataset<Row> streamingDf = spark.readStream().format("rate").load();
        StreamingQuery stream = streamingDf.selectExpr("value as id").writeStream().format("delta").
                option("checkpointLocation", "/tmp/checkpoint").start(location);

        spark.close();
    }
}
