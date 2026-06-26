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
		Channel channel = null;
		try {
			channel = RabbitMQUtil.getChannel();

			channel.exchangeDeclare("order.exchange", "direct");

			channel.queueDeclare("order.queue", true, false, false, null);

			// Bind Queue to Exchange
			channel.queueBind("order.queue", "order.exchange", "order.created");
	
			// Publish Message
			channel.basicPublish("order.exchange", "order.created", null, message.getBytes());

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
