package com.app.consumer;

import java.io.IOException;

import com.app.util.RabbitMQUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

//1. Get a Channel from the shared Connection.
//
//2. Declare the queue to ensure it exists. If the queue already exists,
//RabbitMQ ignores the declaration.
//
//3. Create a DeliverCallback. This contains the business logic that should
//run whenever RabbitMQ delivers a message.
//
//4. Register the callback using basicConsume(). From this point onward,
//RabbitMQ continuously listens for new messages and invokes the callback
//automatically whenever one arrives.

public class OrderCreateConsumer {
	public void start() {

		try {

			// we are getting the channel
			Channel channel = RabbitMQUtil.getChannel();

			// Declaring the queue
			channel.queueDeclare("queue1", true, false, false, null);
			
			System.out.println("Waiting for queue1 message...");

			// using the call back
			DeliverCallback callback = (consumerTag, delivery) -> {

			    try {

			        // Convert byte[] to String
			        String message = new String(delivery.getBody());

			        System.out.println("=================================");
			        System.out.println("CREATE ORDER SERVICE");
			        System.out.println("Consumer Tag : " + consumerTag);
			        System.out.println("Routing Key  : " + delivery.getEnvelope().getRoutingKey());
			        System.out.println("Exchange     : " + delivery.getEnvelope().getExchange());
			        System.out.println("Delivery Tag : " + delivery.getEnvelope().getDeliveryTag());
			        System.out.println("Message      : " + message);
			        System.out.println("=================================");

			        // ----------------------------
			        // Business Logic
			        // ----------------------------

			        System.out.println("Sending Order Create...");
			        Thread.sleep(2000);     // Simulate email sending
			        System.out.println("Order Create Sent Successfully.");

			        // ----------------------------
			        // ACK
			        // ----------------------------

			        channel.basicAck(
			                delivery.getEnvelope().getDeliveryTag(),
			                false);

			    } catch (Exception e) {

			        System.out.println("Order Create Sending Failed!");

			        // Reject the message
			        channel.basicNack(
			                delivery.getEnvelope().getDeliveryTag(),
			                false,
			                true); // true = Requeue

			        e.printStackTrace();
			    }

			};

			// telling what kind of exchange do you want
			channel.exchangeDeclare("pattern.exchange", "topic");

			// binding the queue
			channel.queueBind("queue1", "pattern.exchange", "order.*");
			
			// making the queue consuming ready

			channel.basicConsume("queue1", false, callback, consumerTag->{});
			
			

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
