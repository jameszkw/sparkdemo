package com.zkw.sparkdemo.akka;

import akka.actor.Props;
import akka.japi.Creator;

/**
 * Created by Administrator on 2016/11/29 0029.
 */
public class MyActorC implements Creator {
    Props props1 = Props.create(MyUntypedActor.class);
    Props props2 = Props.create(MyActor.class, "...");
    Props props3 = Props.create(new MyActorC());

    public MyActor create() throws Exception {
        return new MyActor();
    }


}
