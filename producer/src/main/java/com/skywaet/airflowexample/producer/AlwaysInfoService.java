package com.skywaet.airflowexample.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlwaysInfoService implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(AlwaysInfoService.class);

    @Override
    public void run() {
        try {
            while (true) {
                log.info("This is info service");
                Thread.sleep(1000L);
            }
        } catch (InterruptedException e) {
            if (Thread.interrupted()) {
                throw new RuntimeException(e);
            }
        }
    }
}
