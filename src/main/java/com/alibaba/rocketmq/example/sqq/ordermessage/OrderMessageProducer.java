package main.java.com.alibaba.rocketmq.example.sqq.ordermessage;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import main.java.com.alibaba.rocketmq.example.sqq.common.ConfigureProperties;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

/**
 * 顺序消费   生产端
 * @author shiqiangqiang
 *
 */
public class OrderMessageProducer {
	
	public static void main(String args[]) throws Exception{
		// 设置groupname
		String group_name = "order_message_producer";
		DefaultMQProducer producer = new DefaultMQProducer(group_name);
		// 设置namesrv地址
		producer.setNamesrvAddr(ConfigureProperties.ROCKETMQ_CLUSTER_NAMESRV_ADDR);
		// 初始化producer
		producer.start();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		/**
		 * 模拟3笔订单
		 */
		String topic = "OrderTopicTest";
		for (int i=0; i<15; i++){
			String dateStr = sdf.format(new Date());
			// 假设同一笔订单需要5个操作，而且必须保证同一笔订单要被顺序消费。
			for (int j=0; j<5; j++){
				// 消息内容 
				String body = dateStr + " 第"+i+"笔订单， 第"+j+"个操作";
				Message msg = new Message(topic, "order_"+i, body.getBytes());
				// 发送数据。如果使用顺序消费，必须自己实现MessageQueueSelector，保证消息进入同一个队列中去
				SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
					@Override
					public MessageQueue select(List<MessageQueue> messageQueueList, Message msg,
							Object arg) {
						Integer id = (Integer)arg;
						return messageQueueList.get(id);
					}
				}, i%4);	// 0是队列的下标
				
				System.out.println(sendResult + ", body:" + body);
			}
		}
		
		// 应用退出后，关闭清理资源
		producer.shutdown();
	} 
	
	
}
