package com.app.consumer;

import java.io.IOException;

import com.app.util.RabbitMQUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

public class OrderUpdateConsumer {
	public void start() {

		try {

			// we are getting the channel
			Channel channel = RabbitMQUtil.getChannel();

			// Declaring the queue
			channel.queueDeclare("queue2", true, false, false, null);

			System.out.println("Waiting for update order notification...");

			// using the call back
			DeliverCallback callback = (consumerTag, delivery) -> {

				try {

					// Convert byte[] to String
					String message = new String(delivery.getBody());

					System.out.println("=================================");
					System.out.println("UPDATE ORDER SERVICE");
					System.out.println("Consumer Tag : " + consumerTag);
					System.out.println("Routing Key  : " + delivery.getEnvelope().getRoutingKey());
					System.out.println("Exchange     : " + delivery.getEnvelope().getExchange());
					System.out.println("Delivery Tag : " + delivery.getEnvelope().getDeliveryTag());
					System.out.println("Message      : " + message);
					System.out.println("=================================");

					// ----------------------------
					// Business Logic
					// ----------------------------

					System.out.println("Sending order update notification...");
					Thread.sleep(2000); // Simulate email sending
					System.out.println("update Sent Successfully.");

					// ----------------------------
					// ACK
					// ----------------------------

					channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

				} catch (Exception e) {

					System.out.println("order update fail Failed!");

					// Reject the message
					channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true); // true = Requeue

					e.printStackTrace();
				}

			};

			// telling what kind of exchange do you want
			channel.exchangeDeclare("pattern.exchange", "topic");

			// binding the queue
			channel.queueBind("queue2", "pattern.exchange", "order.update");

			// making the queue consuming ready

			channel.basicConsume("queue2", false, callback, consumerTag -> {
			});

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
