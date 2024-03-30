package com.skywaet.airflowexample.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class RandomService implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(RandomService.class);

    @Override
    public void run() {
        var random = new Random();
        while (true) {
            var number = random.nextInt(0, 100);
            if (number % 3 == 0) {
                log.info("This is info message");
            }
            if (number % 3 == 1) {
                log.warn("This is warn message");
            }
            if (number % 3 == 2) {
                log.info("This is error message");
            }
            try {
                Thread.sleep(number * 10L);
            } catch (InterruptedException e) {
                if (Thread.interrupted()) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
