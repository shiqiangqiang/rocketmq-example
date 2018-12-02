package main.java.com.alibaba.rocketmq.example.sqq.ordermessage;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import main.java.com.alibaba.rocketmq.example.sqq.common.ConfigureProperties;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * 顺序消费    消费端
 * @author shiqiangqiang
 *
 */
public class OrderMessageConsumer {
	
	public OrderMessageConsumer() throws MQClientException{
		// 设置groupname,要保证应用唯一
		String groupName = "order_message_consumer";
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
		// 设置namesrv地址
		consumer.setNamesrvAddr(ConfigureProperties.ROCKETMQ_CLUSTER_NAMESRV_ADDR);
		// Consumer第一次启动是从队列头部开始消费
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		// 订阅主题,过滤标签
		consumer.subscribe("OrderTopicTest", "*");
		// 注册监听
		consumer.registerMessageListener(new MessageListenerOrderly() {
			private Random random = new Random();
			
			@Override
			public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs,
					ConsumeOrderlyContext context) {
				// 设置自动提交
				context.setAutoCommit(true);
				// 打印收到的消息
				for (MessageExt msg : msgs){
					System.out.println("消费端Consumer1  msgId:" +msg.getMsgId() + ", content:" + new String(msg.getBody()));
				}
				try {
					// 模拟业务逻辑处理中...
					TimeUnit.SECONDS.sleep(random.nextInt(2));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				// 告知MQ该消息已经被消费
				return ConsumeOrderlyStatus.SUCCESS;
			}
		});
		// 初始化consumer
		consumer.start();
		System.out.println("Consumer1 started.");
	}
	
	public static void main(String[] args) {
		try {
			OrderMessageConsumer consumer = new OrderMessageConsumer();
		} catch (MQClientException e) {
			e.printStackTrace();
		}
	}
	
}
