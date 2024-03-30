package com.skywaet.airflowexample.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlwaysErrorService implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(AlwaysErrorService.class);

    @Override
    public void run() {
        try {
            while (true) {
                log.error("This is error service");
                Thread.sleep(5000L);
            }
        } catch (InterruptedException e) {
            if (Thread.interrupted()) {
                throw new RuntimeException(e);
            }
        }
    }
}
