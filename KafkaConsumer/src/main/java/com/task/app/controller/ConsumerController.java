package com.task.app.controller;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.task.app.model.Message;

@RestController
@RequestMapping("kafka")
public class ConsumerController 
{
	Message msg = null;
	
	@KafkaListener(groupId = "test1-1", topics = "test1" , containerFactory = "kafkaListenerContainerFactory")
	public Message getMessage(Message message)
	{
		msg = message ; 
		return msg;
	}
	
	@RequestMapping("/getMessage")
	public Message consumeMessages()
	{
		return msg;	
	}
}
