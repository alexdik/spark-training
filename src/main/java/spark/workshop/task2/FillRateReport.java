package spark.workshop.task2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import spark.workshop.util.SparkHelper;

import static org.apache.spark.sql.functions.*;

public class FillRateReport {
    private SparkHelper spark = new SparkHelper();
    private Dataset<Row> reqDs = spark.getSession().read().json("data/ad-request.json");
    private Dataset<Row> rspDs = spark.getSession().read().json("data/ad-response.json");

    public void showSource() {
        reqDs.show(false);
        rspDs.show(false);
    }

    /*
        TODO: Find publishers with the highest fill rate.

        Fill rate is calculated as ratio of AdResponses to AdRequests per Publisher, represented as float. Min possible value is 0.0, max value is 1.0.

        Records should be sorted with fill rate descending, so Publishers with the highest fill rate would be listed first.

        Expected output:
            +-----------+--------+
            |publisherId|fillRate|
            +-----------+--------+
            |          2|     1.0|
            |          8|     0.8|
            |          7|    0.67|
            |          4|    0.67|
            |          1|    0.57|
            |          3|     0.5|
            |          5|     0.5|
            |          6|     0.4|
            |          9|     0.0|
            |         10|     0.0|
            +-----------+--------+

        Hints:
            • join reqDs and rspDs using auctionId column
            • group by records by publisher
            • calculate ratio between responses and requests per publisher
            • sort output by fill rate descending

     */
    public Dataset<Row> build() {
        return reqDs;
    }
}
