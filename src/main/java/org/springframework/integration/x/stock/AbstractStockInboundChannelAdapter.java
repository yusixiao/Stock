/**
 * Created by Yusi on 15/12/24.
 */

package org.springframework.integration.x.stock;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.http.client.*;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.StringUtils;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractStockInboundChannelAdapter extends MessageProducerSupport {

    private final static AtomicInteger instance = new AtomicInteger();

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();

    private final Object monitor = new Object();

    private final RestTemplate stock;

    protected AbstractStockInboundChannelAdapter(RestTemplate stock){
        this.stock = stock;
        this.stock.setErrorHandler(new DefaultResponseErrorHandler());
        this.setPhase(Integer.MAX_VALUE);
    }

    public void setReadTimeout(int mills) {
        ClientHttpRequestFactory f = getRequestFactory();
        if(f instanceof SimpleClientHttpRequestFactory) {
            ((SimpleClientHttpRequestFactory) f).setReadTimeout(mills);
        }
        else {
            ((HttpComponentsClientHttpRequestFactory) f).setReadTimeout(mills);
        }
    }

    public void setConnectionTimeout(int mills) {
        ClientHttpRequestFactory f = getRequestFactory();
        if(f instanceof SimpleClientHttpRequestFactory) {
            ((SimpleClientHttpRequestFactory) f).setConnectTimeout(mills);
        }
        else {
            ((HttpComponentsClientHttpRequestFactory) f).setConnectTimeout(mills);
        }
    }

    @Override
    protected void onInit() {
        this.taskExecutor.setThreadNamePrefix("Stock-" + instance.incrementAndGet());
        this.taskExecutor.initialize();
    }

    @Override
    protected void doStart() {
        synchronized (this.monitor) {
            if(this.running.get()) {
                // already running
                return ;
            }
            this.running.set(true);
            this.taskExecutor.execute(new StreamReadingTask());
        }
    }

    @Override
    protected void doStop() {
        this.running.set(false);
        this.taskExecutor.getThreadPoolExecutor().shutdownNow();
        try {
            if(!this.taskExecutor.getThreadPoolExecutor().awaitTermination(10, TimeUnit.SECONDS)) {
                logger.error("Reader task failed to stop");
            }
        }catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    protected abstract URI buildUri();

    protected abstract void doSendLine(String line);

    protected class StreamReadingTask implements Runnable{
        public void run() {
            while(running.get()) {

            }
        }

        private void readStream(RestTemplate restTemplate) {
            restTemplate.execute(buildUri(), HttpMethod.GET, new RequestCallback() {
                    public void doWithRequest(ClientHttpRequest clientHttpRequest) throws IOException {
                        }
                    },
                    new ResponseExtractor<String>() {
                        public String extractData(ClientHttpResponse response) throws IOException{
                            InputStream inputStream = response.getBody();
                            LineNumberReader reader = null;
                            try {
                                reader = new LineNumberReader(new InputStreamReader(inputStream));
                                resetBackOffs();
                                while (running.get()) {
                                    String line = reader.readLine();
                                    if (!StringUtils.hasText(line)) {
                                        break;
                                    }
                                    doSendLine(line);
                                }
                            }
                            finally {
                                if (reader != null) {
                                    reader.close();
                                }
                            }

                            return null;
                        }
                    }
            );
        }
    }

    private ClientHttpRequestFactory getRequestFactory() {
        // InterceptingClientHttpRequestFactory doesn't let us access the underlying object
        DirectFieldAccessor f = new DirectFieldAccessor(stock.getRequestFactory());
        Object requestFactory = f.getPropertyValue("requestFactory");
        return (ClientHttpRequestFactory) requestFactory;
    }

    private void resetBackOffs() {
//        linearBackOff.set(250);
//        rateLimitBackOff.set(60000);
//        httpErrorBackOff.set(5000);
    }

    protected void wait(int millis) {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (!this.running.get()) {
                // no longer running
                return;
            }
            throw new IllegalStateException(e);
        }
    }
}

