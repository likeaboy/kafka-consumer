package secfox.soc.consumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 已废弃，请使用com.kafka.perm.consumer.muti.PollThreadHandler
 * @author wangzhijie
 *
 */
@Deprecated
public class ConsumerHandler implements Runnable{

	private final static Logger logger = LoggerFactory.getLogger(ConsumerHandler.class);
	
	private final KafkaConsumer<String, Object> consumer;
	//暂时不考虑开启多个worker处理写es
	private ExecutorService executors;
	private TransportClient client;
	private static final int BULK_SIZE=6400;
	
	public ConsumerHandler(TransportClient client){
		this.client = client;
		TConsumerConfig tconf = TConsumerConfig.getInstance();
		  Properties props = new Properties();
	      props.put("bootstrap.servers", tconf.getBrokerAddr());
	      props.put("group.id", tconf.getEsConsumerGroup());
	      props.put("enable.auto.commit", "true"); 
	      props.put("auto.commit.interval.ms", "1000");
	      props.put("session.timeout.ms", "30000");
	      props.put("auto.offset.reset", "earliest");
	      
	      //要发送自定义对象，需要指定对象的反序列化类
	      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	      props.put("value.deserializer", tconf.getVSerialClass());
//	      props.put("value.deserializer", "com.kafka.test.kfk_test.serial.DecodeingKafka");
	      
	      //使用String时可以使用系统的反序列化类
//	      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//	 	  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	      consumer = new KafkaConsumer<String, Object>(props);
	      consumer.subscribe(Arrays.asList(tconf.getTopic()));
	      executors = Executors.newFixedThreadPool(tconf.getCoreSize());
//	      executors = Executors.newFixedThreadPool(tconf.getWorkers());
	}
	
	@Override
	public void run() {
		logger.info(Thread.currentThread().getName() +" consumer handler thread start ...");
		//已消费条数，注意long值得上限
		long consumeCount=0L;
		//是否存在bulk request
		boolean toCreate=false;
		BulkRequestBuilder bulkRequest=null;
		while(true){
			ConsumerRecords<String, Object> records = consumer.poll(200);
			if(records.count()==0 && consumeCount>0 && consumeCount<BULK_SIZE){
				try{
					bulkRequest.execute().actionGet();
					toCreate=false;
					logger.info(Thread.currentThread().getName() + " bulk request commit... consumeCount" + consumeCount);
				}catch(Exception e){
					//batch commit error，暂时不处理
					logger.error("batch commit error",e);
					//可以考虑开启异步线程，将error数据回补到es
					toCreate=false;
				}
				consumeCount=0;
			}
			if(records.count()==0)
				continue;
			if(!toCreate){
				//创建bulk请求
				bulkRequest=client.prepareBulk();
				toCreate=true;
			}
			//将poll到的数据添加到当前bulk request中
			for(ConsumerRecord<String, Object> r : records){
				bulkRequest.add(client.prepareIndex("data_1","sim").setSource((String)r.value()));
			}
			
			consumeCount=consumeCount+records.count();
			logger.info("cc:" + consumeCount);
			if((consumeCount-BULK_SIZE)>0){
				try{
					/*if (bulkResponse.hasFailures()) {

		            BulkItemResponse[] items = bulkResponse.getItems();
		            for (BulkItemResponse item : items) {
		                System.out.println(item.getFailureMessage());
		            }

		        } else {
		            System.out.println("全部执行成功！");
		        }*/
					bulkRequest.execute().actionGet();
					toCreate=false;
					logger.info(Thread.currentThread().getName() +" bulk request commit... consumeCount" + consumeCount);
				}catch(Exception e){
					//batch commit error，暂时不处理
					logger.error("batch commit error",e);
					//可以考虑开启异步线程，将error数据回补到es
					toCreate=false;
				}
				consumeCount=consumeCount-BULK_SIZE;
			}
		}
	}
	
	public void execute(){
		logger.info(Thread.currentThread().getName() +" consumer handler thread start ...");
		//已消费条数，注意long值得上限
		long consumeCount=0L;
		//是否存在bulk request
		boolean toCreate=false;
		BulkRequestBuilder bulkRequest=null;
		while(true){
			ConsumerRecords<String, Object> records = consumer.poll(200);
			if(records.count()==0 && consumeCount>0 && consumeCount<BULK_SIZE){
				try{
					bulkRequest.execute().actionGet();
					toCreate=false;
					logger.info(Thread.currentThread().getName() + " bulk request commit... consumeCount" + consumeCount);
				}catch(Exception e){
					//batch commit error，暂时不处理
					logger.error("batch commit error",e);
					//可以考虑开启异步线程，将error数据回补到es
					toCreate=false;
				}
				consumeCount=0;
			}
			if(records.count()==0)
				continue;
			if(!toCreate){
				//创建bulk请求
				bulkRequest=client.prepareBulk();
				toCreate=true;
			}
			//将poll到的数据添加到当前bulk request中
			for(ConsumerRecord<String, Object> r : records){
				bulkRequest.add(client.prepareIndex("data_1","sim").setSource((String)r.value()));
			}
			
			consumeCount=consumeCount+records.count();
			logger.info("cc:" + consumeCount);
			if((consumeCount-BULK_SIZE)>0){
				try{
					bulkRequest.execute().actionGet();
					toCreate=false;
					logger.info(Thread.currentThread().getName() +" bulk request commit... consumeCount" + consumeCount);
				}catch(Exception e){
					//batch commit error，暂时不处理
					logger.error("batch commit error",e);
					//可以考虑开启异步线程，将error数据回补到es
					toCreate=false;
				}
				consumeCount=consumeCount-BULK_SIZE;
			}
		}
	}
	
	public void shutdown(){
		if(consumer != null){
			consumer.close();
		}
		if(executors != null){
			executors.shutdown();
		}
		try {
			if (!executors.awaitTermination(10, TimeUnit.SECONDS)) {
				 System.out.println("Timeout.... Ignore for this case");
			}
		} catch (InterruptedException e) {
			System.out.println("Other thread interrupted this shutdown, ignore for this case.");
			Thread.currentThread().interrupt();
			e.printStackTrace();
		}
	}
}
