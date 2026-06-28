package com.app.servlet;

import java.io.IOException;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.app.util.RabbitMQUtil;
import com.rabbitmq.client.Channel;

@WebServlet("/send")
public class ProducerServlet extends HttpServlet {

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse res) {

		String message = req.getParameter("msg");
		String routingKey = req.getParameter("key");
		if (message == null || message.isBlank()) {
			try {
				res.getWriter().println("Message cannot be empty");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return;
		}
		
		
		 if (routingKey == null || routingKey.isBlank()) {
	            try {
					res.getWriter().println("Routing key cannot be empty");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	            return;
	        }
		Channel channel = null;
		try {
			channel = RabbitMQUtil.getChannel();

			// declaring the exchange
			channel.exchangeDeclare("pattern.exchange", "topic");

			// declaring the queue
			channel.queueDeclare("queue1", true, false, false, null);

			channel.queueDeclare("queue2", true, false, false, null);

			channel.queueDeclare("queue3", true, false, false, null);

			// Bind Queue to Exchange
			
			channel.queueBind("queue1", "pattern.exchange", "order.*");

			channel.queueBind("queue2", "pattern.exchange", "order.update");

			channel.queueBind("queue3", "pattern.exchange", "#");

			// Publish Message
			channel.basicPublish("pattern.exchange", routingKey, null, message.getBytes());	

			res.getWriter().println("Message Sent Successfully");

		} catch (IOException e) {
			try {
				res.getWriter().println("Error while processing the Message");
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			RabbitMQUtil.closeChannel(channel);

		}
	}
}
