package com.zkw.sparkdemo.akka;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

/**
 * Created by Administrator on 2016/11/28 0028.
 */
public class MyUntypedActor extends UntypedActor {
    LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof String) {
            log.info("Received String message: {}", message);
            getSender().tell(message, getSelf());
        } else
            unhandled(message);
    }
}
