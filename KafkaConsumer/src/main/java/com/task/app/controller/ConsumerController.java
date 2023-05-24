package com.task.app.controller;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.task.app.model.Message;

@RestController
@RequestMapping("kafka")
public class ConsumerController 
{
	Message msg = null;
	
	@KafkaListener(groupId = "test1-1", topics = "test1" , containerFactory = "kafkaListenerContainerFactory")
	public Message getMessage1(Message message)
	{
		msg = message ; 
		return msg;
	}
	
	@KafkaListener(groupId = "test2-2", topics = "test2" , containerFactory = "kafkaListenerContainerFactory")
	public Message getMessage2(Message message)
	{
		msg = message ; 
		setFromKafkatoCassandra(msg);
		return msg;
	}
	
	@RequestMapping("/getMessage")
	public Message consumeMessages()
	{
		return msg;	
	}
	
	private void setFromKafkatoCassandra(Message msg)
	{
		int id =  msg.getId();
		String val = msg.getMessage();
		RestTemplate temp = new RestTemplate();
		temp.getForObject("http://localhost:8085/cassandra/save?id="+id+"&message="+val+"", Message.class);
	}
}
