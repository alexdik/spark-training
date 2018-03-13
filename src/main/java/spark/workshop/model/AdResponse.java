package spark.workshop.model;

public class AdResponse {
    private String auctionId;
    private Long advertiserId;
    private Long price;

    public AdResponse(String auctionId, Long advertiserId, Long price) {
        this.auctionId = auctionId;
        this.advertiserId = advertiserId;
        this.price = price;
    }

    public AdResponse() {
    }

    public String getAuctionId() {
        return auctionId;
    }

    public void setAuctionId(String auctionId) {
        this.auctionId = auctionId;
    }

    public Long getAdvertiserId() {
        return advertiserId;
    }

    public void setAdvertiserId(Long advertiserId) {
        this.advertiserId = advertiserId;
    }

    public Long getPrice() {
        return price;
    }

    public void setPrice(Long price) {
        this.price = price;
    }
}
