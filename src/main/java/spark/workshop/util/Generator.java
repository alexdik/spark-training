package spark.workshop.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.types.ObjectId;
import scala.Tuple3;
import spark.workshop.model.AdRequest;
import spark.workshop.model.AdResponse;
import spark.workshop.model.Impression;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.Random;

class Generator {
    private final static Random RAND = new Random();
    private final static ObjectMapper MAPPER = new ObjectMapper();
    private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private final static int AD_REQUEST_COUNT = 50;

    public static void main(String[] args) {
        Generator gen = new Generator();
        gen.generateFiles();
    }

    private Tuple3<AdRequest, Optional<AdResponse>, Optional<Impression>> generateAuction() {
        String auctionId = new ObjectId().toString();
        Long advertiserId = RAND.nextInt(10) + 1L;
        Long publisherId = RAND.nextInt(10) + 1L;
        Long price = (long) Math.max(RAND.nextInt(1000), 50);
        Long latencyMilliseconds = (long) Math.max(RAND.nextInt(1000), 50);
        Date date = new Date(System.currentTimeMillis() - (RAND.nextInt(3) * 86400000));
        boolean hasResponse = RAND.nextBoolean();
        boolean hasImpression = hasResponse && RAND.nextBoolean();

        return new Tuple3<>(
                new AdRequest(auctionId, publisherId, DATE_FORMAT.format(date)),
                hasResponse ? Optional.of(new AdResponse(auctionId, advertiserId, price)) : Optional.empty(),
                hasImpression ? Optional.of(new Impression(auctionId, latencyMilliseconds)) : Optional.empty()
        );
    }

    private void generateFiles() {
        StringBuilder sbReq = new StringBuilder();
        StringBuilder sbRsp = new StringBuilder();
        StringBuilder sbImp = new StringBuilder();

        for (int cnt = 0; cnt < AD_REQUEST_COUNT; cnt++) {
            Tuple3<AdRequest, Optional<AdResponse>, Optional<Impression>> auction = generateAuction();
            sbReq.append(toJson(auction._1())).append("\n");
            auction._2().ifPresent(adResponse -> sbRsp.append(toJson(adResponse)).append("\n"));
            auction._3().ifPresent(impression -> sbImp.append(toJson(impression)).append("\n"));
        }

        System.out.println(sbReq.toString());
        System.out.println(sbRsp.toString());
        System.out.println(sbImp.toString());
    }

    private String toJson(Object object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
