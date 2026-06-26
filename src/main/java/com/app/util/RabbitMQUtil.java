package com.app.util;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQUtil {

	// One factory for the entire application
	private static final ConnectionFactory factory = new ConnectionFactory();

	// One shared TCP connection
	private static Connection connection;

	// Static block executes only once
	static {

		try {

			factory.setHost("localhost");
			factory.setUsername("guest");
			factory.setPassword("guest");

			connection = factory.newConnection();

			System.out.println("RabbitMQ Connection Established Successfully.");

		} catch (IOException | TimeoutException e) {
			
			System.out.println(e.getMessage());
			throw new RuntimeException("Failed to connect to RabbitMQ", e);

		}

	}

	public static Connection getConnection() {

		return connection;

	}

	public static Channel getChannel() throws IOException {

		return connection.createChannel();

	}

	public static void closeChannel(Channel channel) {

		if (channel != null && channel.isOpen()) {

			try {
				channel.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	/**
	 * Closes the shared connection.
	 */
	public static void closeConnection() {

		if (connection != null && connection.isOpen()) {

			try {
				connection.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

}