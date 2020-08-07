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

            System.out.println("INIT_DATA");
            Dataset<Long> data = spark.range(0, 5);
            data.write().format("delta").save(location);
            Dataset<Row> df = spark.read().format("delta").load(location);
            df.show();

            //StreamingQuery stream = spark.readStream().format("delta").
            //        load(location).writeStream().
            //        format("console").start();
            //System.out.println(stream.toString());

            System.out.println("OVERWRITE_DATA");
            data = spark.range(5, 10);
            location = "/tmp/delta-table-"+ UUID.randomUUID().toString();
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


            System.out.println("TIMETRAVEL_DATA");
            df = spark.read().format("delta").option("versionAsOf", 0).load(location);
            df.show();

            System.out.println("TIMETRAVEL_DATA");
            df = spark.read().format("delta").option("versionAsOf", 1).load(location);
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

        Dataset<Row> streamingDf = spark.readStream().format("rate").load();
        //streamingDf.show();
        StreamingQuery stream = streamingDf.selectExpr("value as id").writeStream().
                format("delta").
                option("checkpointLocation", "/tmp/checkpoint").start(location);

        //spark.readStream().format("delta").
        //        load(location).writeStream().
        //        format("console").start();

        spark.close();
    }
}
