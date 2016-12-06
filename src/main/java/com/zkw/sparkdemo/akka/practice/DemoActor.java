package com.zkw.sparkdemo.akka.practice;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;

/**
 * Created by Administrator on 2016/11/29 0029.
 */
public class DemoActor extends UntypedActor {
    private static ActorSystem actorSystem = ActorSystem.create();

    final int magicNumber;

    public DemoActor(int magicNumber) {
        this.magicNumber = magicNumber;
    }

    /**
     * Create Props for an actor of this type.
     * @param magicNumber The magic number to be passed to this actor’s constructor.
     * @return a Props for creating this actor, which can then be further configured
     *         (e.g. calling `.withDispatcher()` on it)
     */
    public static Props props(final int magicNumber) {
        return Props.create(new Creator<DemoActor>() {
            private static final long serialVersionUID = 1L;

            public DemoActor create() throws Exception {
                return new DemoActor(magicNumber);
            }
        });
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        // some behavior here
        System.out.println("some behavior here receive... ...");
    }
    public static ActorRef actorOf() {
        return actorSystem.actorOf(DemoActor.props(42), "demo");
    }
}
