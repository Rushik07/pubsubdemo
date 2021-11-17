package com.example.pubsubdemo;

import com.solace.services.core.model.SolaceServiceCredentials;
import com.solacesystems.jcsmp.*;
import lombok.extern.apachecommons.CommonsLog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@SpringBootApplication
@CommonsLog
public class PubsubdemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(PubsubdemoApplication.class, args);
    }

    @Component
    static class Runner implements CommandLineRunner {
        //JCMPFactory is used to obtain instances of messaging system entities. Creating topic as well.
        private final Topic topic = JCSMPFactory.onlyInstance().createTopic("tutorial/topic");

        @Autowired
        private SpringJCSMPFactory solaceFactory;

        // Examples of other beans that can be used together to generate a customized SpringJCSMPFactory
        @Autowired(required=false) private SpringJCSMPFactoryCloudFactory springJCSMPFactoryCloudFactory;
        @Autowired(required=false) private SolaceServiceCredentials solaceServiceCredentials;
        @Autowired(required=false) private JCSMPProperties jcsmpProperties;

        private demoMessageConsumer msgConsumer = new demoMessageConsumer();
        private demoPublishEventHandler pubEventHandler = new demoPublishEventHandler();

        public void run(String... strings) throws Exception {
            final String msg = "Hello World";
            final JCSMPSession session = solaceFactory.createSession();

            XMLMessageConsumer cons = session.getMessageConsumer(msgConsumer);
            //adds a subscription to the appliance.
            session.addSubscription(topic);
            log.info("Connected. Awaiting message...");
            //start receiving message
            cons.start();

            // Consumer session is now hooked up and running!

            /** Anonymous inner-class for handling publishing events */
            XMLMessageProducer prod = session.getMessageProducer(pubEventHandler);

            // Publish-only session is now hooked up and running!
            //Creates a message instance tied to that producer.

            TextMessage jcsmpMsg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);

            //sets message content.

            jcsmpMsg.setText(msg);

            //sets persistent delivery mode.
            //persistent(Guaranteed message) , sends message even if receiver is offline.
            //keeps copy until successfully deliverd.
            jcsmpMsg.setDeliveryMode(DeliveryMode.PERSISTENT);

            log.info("============= Sending " + msg);
            prod.send(jcsmpMsg, topic);

            try {
                // block here until message received, and latch will flip.
                msgConsumer.getLatch().await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("I was awoken while waiting``");
            }
            // Close consumer
            cons.close();
            log.info("Exiting.");
            session.closeSession();
        }
    }

}
