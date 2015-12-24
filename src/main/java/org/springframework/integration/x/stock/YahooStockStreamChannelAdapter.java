package org.springframework.integration.x.stock;

import java.net.URI;

import org.springframework.integration.support.MessageBuilder;
import org.springframework.social.support.URIBuilder;
import org.springframework.web.client.RestTemplate;

/**
 * Created by Yusi on 15/12/24.
 */
public class YahooStockStreamChannelAdapter extends AbstractStockInboundChannelAdapter {

    private static final String YAHOO_URL = "http://finance.yahoo.com/d/";

    protected YahooStockStreamChannelAdapter(RestTemplate stock) {
        super(stock);
    }

    @Override
    protected URI buildUri() {
        String path = "quotes.csv";

        URIBuilder b = URIBuilder.fromUri(YAHOO_URL + path);

        b.queryParam("s", "BIDU");
        b.queryParam("f", "snd1l1yr");

        return b.build();
    }

    @Override
    protected void doSendLine(String line) {
        sendMessage(MessageBuilder.withPayload(line).build());
    }
}
