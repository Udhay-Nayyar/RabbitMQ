package com.app.consumer;

import java.io.IOException;

import com.app.util.RabbitMQUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

public class OrderDeleteConsumer {
	public void start() {

		try {

			// we are getting the channel
			Channel channel = RabbitMQUtil.getChannel();

			// Declaring the queue
			channel.queueDeclare("queue3", true, false, false, null);

			System.out.println("Waiting for Order Delete...");

			// using the call back
			DeliverCallback callback = (consumerTag, delivery) -> {

				try {

					// Convert byte[] to String
					String message = new String(delivery.getBody());

					System.out.println("=================================");
					System.out.println("DELETE ORDER SERVICE");
					System.out.println("Consumer Tag : " + consumerTag);
					System.out.println("Routing Key  : " + delivery.getEnvelope().getRoutingKey());
					System.out.println("Exchange     : " + delivery.getEnvelope().getExchange());
					System.out.println("Delivery Tag : " + delivery.getEnvelope().getDeliveryTag());
					System.out.println("Message      : " + message);
					System.out.println("=================================");

					// ----------------------------
					// Business Logic
					// ----------------------------

					System.out.println("Sending order delete ...");
					Thread.sleep(2000); // Simulate email sending
					System.out.println("Delete Sent Successfully.");

					// ----------------------------
					// ACK
					// ----------------------------

					channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

				} catch (Exception e) {

					System.out.println("Order delete Sending Failed!");

					// Reject the message
					channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true); // true = Requeue

					e.printStackTrace();
				}

			};

			// telling what kind of exchange do you want
			channel.exchangeDeclare("pattern.exchange", "topic");

			// binding the queue
			channel.queueBind("queue3", "pattern.exchange", "#");

			// making the queue consuming ready

			channel.basicConsume("queue3", false, callback, consumerTag -> {
			});

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
