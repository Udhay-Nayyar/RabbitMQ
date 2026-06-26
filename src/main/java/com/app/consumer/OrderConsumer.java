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

public class OrderConsumer {
	public void start() {

		try {
			
			
			// we are getting the channel 
			Channel channel = RabbitMQUtil.getChannel();

			
			// Declaring the queue 
			channel.queueDeclare("order.queue", true, false, false, null);

			System.out.println("Waiting for messages...");

			
			
			// using the call back 
			DeliverCallback callback = (consumerTag, delivery) -> {

				String message = new String(delivery.getBody());

				System.out.println("Received : " + message);

				channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

			};

			// telling what kind of exchange do you want
			channel.exchangeDeclare("order.exchange", "direct");
			
			
			// binding the queue
			channel.queueBind("order.queue", "order.exchange", "order.created");
			
			
			// making the queue consuming ready 
			channel.basicConsume("order.queue", false, callback, consumerTag -> {});

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
