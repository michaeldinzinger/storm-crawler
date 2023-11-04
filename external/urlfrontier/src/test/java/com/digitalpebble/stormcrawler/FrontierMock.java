package com.digitalpebble.stormcrawler;

import crawlercommons.urlfrontier.URLFrontierGrpc.URLFrontierImplBase;
import crawlercommons.urlfrontier.Urlfrontier.AckMessage;
import crawlercommons.urlfrontier.Urlfrontier.AckMessage.Builder;
import crawlercommons.urlfrontier.Urlfrontier.AckMessage.Status;
import crawlercommons.urlfrontier.Urlfrontier.URLItem;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FrontierMock extends URLFrontierImplBase implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(FrontierMock.class);

    private static boolean isClosed = true;

    @Override
    public StreamObserver<URLItem> putURLs(
            StreamObserver<crawlercommons.urlfrontier.Urlfrontier.AckMessage> responseObserver) {

        return new StreamObserver<URLItem>() {

            final AtomicInteger unacked = new AtomicInteger();

            @Override
            public void onNext(URLItem value) {
                String url;
                if (value.hasDiscovered()) {
                    url = value.getDiscovered().getInfo().getUrl();
                } else {
                    url = value.getKnown().getInfo().getUrl();
                }

                final Builder ack = AckMessage.newBuilder();
                if (value.getID() == null || value.getID().isEmpty()) {
                    ack.setID(url);
                } else {
                    ack.setID(value.getID());
                }

                // do not add new stuff if we are in the process of closing
                if (isClosed()) {
                    responseObserver.onNext(ack.setStatus(Status.FAIL).build());
                    return;
                }

                unacked.incrementAndGet();

                // final Status status = putURLItem(value);
                final Status status = Status.OK;
                LOG.debug("putURL -> {} got status {}", url, status);
                final AckMessage ackedMessage = ack.setStatus(status).build();
                responseObserver.onNext(ackedMessage);
                unacked.decrementAndGet();
            }

            @Override
            public void onError(Throwable t) {
                if (t instanceof StatusRuntimeException) {
                    // ignore messages about the client having cancelled
                    if (((StatusRuntimeException) t)
                            .getStatus()
                            .getCode()
                            .equals(io.grpc.Status.Code.CANCELLED)) {
                        return;
                    }
                }
                LOG.error("Error reported {}", t.getMessage());
            }

            @Override
            public void onCompleted() {
                // will this ever get called if the client is constantly streaming?
                // check that all the work for this stream has ended
                while (unacked.get() != 0) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                responseObserver.onCompleted();
            }
        };
    }

    private boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() throws IOException {
        isClosed = true;
    }
}
