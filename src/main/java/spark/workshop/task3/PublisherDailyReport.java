package spark.workshop.task3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import spark.workshop.util.SparkHelper;

import static org.apache.spark.sql.functions.*;

public class PublisherDailyReport {
    private SparkHelper spark = new SparkHelper();
    private Dataset<Row> reqDs = spark.getSession().read().json("data/ad-request.json");
    private Dataset<Row> rspDs = spark.getSession().read().json("data/ad-response.json");
    private Dataset<Row> impDs = spark.getSession().read().json("data/impression.json");

    public void showSource() {
        reqDs.show(false);
        rspDs.show(false);
        impDs.show(false);
    }

    /*
        TODO: Show daily statistics per Publisher.

        Report should have following columns:
            • publisher id
            • day (as formatted string yyyy-MM-dd)
            • requests count
            • responses count
            • impressions count

        Expected output:
            +-----------+----------+------------+-------------+---------------+
            |publisherId|       day|requestCount|responseCount|impressionCount|
            +-----------+----------+------------+-------------+---------------+
            |          1|2018-03-11|           2|            2|              1|
            |          1|2018-03-12|           2|            0|              0|
            |          1|2018-03-13|           2|            1|              0|
            |          2|2018-03-12|           2|            2|              1|
            |          2|2018-03-13|           1|            1|              1|
            |          3|2018-03-11|           6|            4|              4|
            |          3|2018-03-12|           6|            2|              1|
            |          3|2018-03-13|           2|            0|              0|
            |          4|2018-03-11|           2|            2|              0|
            |          4|2018-03-12|           1|            1|              0|
            |          4|2018-03-13|           1|            0|              0|
            |          5|2018-03-11|           3|            0|              0|
            |          5|2018-03-12|           2|            2|              0|
            |          6|2018-03-11|           1|            1|              0|
            |          6|2018-03-12|           1|            0|              0|
            |          6|2018-03-13|           3|            1|              0|
            |          7|2018-03-11|           3|            3|              2|
            |          7|2018-03-13|           1|            1|              1|
            |          8|2018-03-12|           1|            1|              1|
            |          8|2018-03-13|           1|            1|              0|
            |          9|2018-03-12|           3|            2|              2|
            |          9|2018-03-13|           1|            1|              0|
            |         10|2018-03-12|           2|            2|              1|
            |         10|2018-03-13|           1|            0|              0|
            +-----------+----------+------------+-------------+---------------+

        Hints:
            • Join 3 datasets reqDs, rspDs, impDs via auctionId column
            • add day column to dataset populated as substring from date column
            • aggregate dataset by publisherId and day
            • calculate total count number of records for each dataset

     */
    public Dataset<Row> build() {
        return reqDs;
    }
}
