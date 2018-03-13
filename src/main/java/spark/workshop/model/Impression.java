package spark.workshop.model;

public class Impression {
    private String auctionId;
    private Long latencyMilliseconds;

    public Impression(String auctionId, Long latencyMilliseconds) {
        this.auctionId = auctionId;
        this.latencyMilliseconds = latencyMilliseconds;
    }

    public Impression() {
    }

    public String getAuctionId() {
        return auctionId;
    }

    public void setAuctionId(String auctionId) {
        this.auctionId = auctionId;
    }

    public Long getLatencyMilliseconds() {
        return latencyMilliseconds;
    }

    public void setLatencyMilliseconds(Long latencyMilliseconds) {
        this.latencyMilliseconds = latencyMilliseconds;
    }
}
