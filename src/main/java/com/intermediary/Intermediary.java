package com.intermediary;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.Date;

public class Intermediary {

    private final static String QUEUE_NAME_TOGET = "publisher_message";
    private final static String QUEUE_NAME_TOSENT = "reciever_message";

    static Date date;

    public static void main(String[] args)
    {
        getMessage();
    }

    static String messageModificate(String message)
    {
        date = new Date();
        message += " " + date.toString();
        return message;
    }

    static void getMessage()  {
        try
        {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare(QUEUE_NAME_TOGET, false, false, false, null);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");

                message = messageModificate(message);

                sendMessage(message);
            };

            channel.basicConsume(QUEUE_NAME_TOGET, true, deliverCallback, consumerTag -> { });
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
    }

    static void sendMessage(String messageToSend)
    {
        try
        {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {

                channel.queueDeclare(QUEUE_NAME_TOSENT, false, false, false, null);
                channel.basicPublish("", QUEUE_NAME_TOSENT, null, messageToSend.getBytes());
            }
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
    }
}
