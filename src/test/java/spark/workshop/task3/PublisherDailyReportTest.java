package spark.workshop.task3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PublisherDailyReportTest {
    private static Dataset<Row> ds;

    @BeforeClass
    public static void setUp() {
        PublisherDailyReport publisherDailyReport = new PublisherDailyReport();
        publisherDailyReport.showSource();
        ds = publisherDailyReport.build();
    }

    @Test
    public void has5Columns() {
        assertThat(ds.columns()).containsOnly("publisherId", "day", "requestCount", "responseCount", "impressionCount");
    }

    @Test
    public void totalRequestCount() {
        Row firstRow = ds.groupBy().sum("requestCount").collectAsList().get(0);
        assertThat(firstRow.getLong(0)).isEqualTo(50);
    }

    @Test
    public void totalResponseCount() {
        Row firstRow = ds.groupBy().sum("responseCount").collectAsList().get(0);
        assertThat(firstRow.getLong(0)).isEqualTo(30);
    }

    @Test
    public void totalImpressionCount() {
        Row firstRow = ds.groupBy().sum("impressionCount").collectAsList().get(0);
        assertThat(firstRow.getLong(0)).isEqualTo(15);
    }

}
