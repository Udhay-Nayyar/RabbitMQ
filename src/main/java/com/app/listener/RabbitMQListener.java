package com.app.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

import com.app.consumer.OrderCreateConsumer;
import com.app.consumer.OrderUpdateConsumer;
import com.app.consumer.OrderDeleteConsumer;

@WebListener
public class RabbitMQListener implements ServletContextListener {

    
	@Override
	public void contextInitialized(ServletContextEvent sce)
	{
		System.out.println("consumers are now alive");
		(new OrderDeleteConsumer()).start();
		(new OrderUpdateConsumer()).start();
		(new OrderCreateConsumer()).start();
	}
	
	
	
	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		System.out.println("Destroying the consumers");
	}
	
}
