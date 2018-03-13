package spark.workshop.model;

public class AdRequest {
    private String auctionId;
    private Long publisherId;
    private String date;

    public AdRequest() {
    }

    public AdRequest(String auctionId, Long publisherId, String date) {
        this.auctionId = auctionId;
        this.publisherId = publisherId;
        this.date = date;
    }

    public String getAuctionId() {
        return auctionId;
    }

    public Long getPublisherId() {
        return publisherId;
    }

    public String getDate() {
        return date;
    }

    public void setAuctionId(String auctionId) {
        this.auctionId = auctionId;
    }

    public void setPublisherId(Long publisherId) {
        this.publisherId = publisherId;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
