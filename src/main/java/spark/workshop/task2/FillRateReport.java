package spark.workshop.task2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import spark.workshop.util.Registry;
import spark.workshop.util.SparkHelper;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.round;

public class FillRateReport {
    private SparkHelper spark = new SparkHelper();
    private Dataset<Row> reqDs = spark.session.read().json("data/ad-request.json");
    private Dataset<Row> rspDs = spark.session.read().json("data/ad-response.json");

    public FillRateReport() {
        Registry registry = new Registry();
        UDF1<Long, String> getPublisherName = registry::getPublisherName;
        spark.session.udf().register("getPublisherName", getPublisherName, DataTypes.StringType);
    }

    public void showSource() {
        reqDs.show(false);
        rspDs.show(false);
    }

    /*
        TODO: Find publishers with the highest fill rate.

        Fill rate is calculated as ratio of AdResponses to AdRequests per Publisher, represented as float. Min possible value is 0.0, max value is 1.0.

        Records should be sorted by fill rate descending.

        Expected output:
        +---------------+--------+
        |      publisher|fillRate|
        +---------------+--------+
        |     RELX Group|     1.0|
        |    McGraw-Hill|     1.0|
        |  Grupo Planeta|     1.0|
        |    Bertelsmann|    0.75|
        |          Wiley|    0.75|
        |Springer Nature|    0.67|
        |        Pearson|     0.5|
        | ThomsonReuters|    0.43|
        | Wolters Kluwer|     0.4|
        | Hachette Livre|     0.4|
        +---------------+--------+

        Hints:
            1) left join reqDs and rspDs using auctionId column
            2) for each row add column hasResponse calculated as following: if response exist hasResponse == 1, otherwise hasResponse == 0
            3) group by publisherId
            4) aggregate and calculate ratio between total responses (sum of hasResponse) and requests count
            5) sort output by fill rate descending
            6) map publisherId to publisher name using UDF "getPublisherName"
    */
    public Dataset<Row> build() {
        return reqDs.join(rspDs, reqDs.col("auctionId").equalTo(rspDs.col("auctionId")), "left_outer")
            .withColumn("hasResponse", when(col("advertiserId").isNotNull(), 1).otherwise(0))
            .select(callUDF("getPublisherName", col("publisherId")).as("publisher"), col("hasResponse"))
            .groupBy(col("publisher"))
            .agg(round(sum("hasResponse").divide(count("hasResponse")).as("fillRate"), 2).as("fillRate"))
            .sort(desc("fillRate"));
    }
}
