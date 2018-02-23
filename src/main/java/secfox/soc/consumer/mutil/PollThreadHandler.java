package secfox.soc.consumer.mutil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import secfox.soc.consumer.TConsumerConfig;

/**
 * poll thread，定时轮询kafka mq获取event
 * @author wangzhijie
 *
 */
public class PollThreadHandler implements Runnable{
	
private final static Logger logger = LoggerFactory.getLogger(PollThreadHandler.class);
	
	private final KafkaConsumer<String, String> consumer;
	//worker线程池，执行具体处理逻辑
	private ExecutorService workers;
	//任务队列
	private LinkedBlockingQueue<Runnable> taskQueue;
	private TransportClient client;
	//es批量提交大小
	private static final int BULK_SIZE=6400;
	private TConsumerConfig tconf;
	//任务队列容量
//	private int taskQueueCapacity = tconf.getMaxQueueSize();
	private ReentrantLock pollLock = new ReentrantLock();
	private Condition notFull = pollLock.newCondition();

	public PollThreadHandler(TransportClient client){
		tconf = TConsumerConfig.getInstance();
		this.client = client;
		
		  Properties props = new Properties();
	      props.put("bootstrap.servers", tconf.getBrokerAddr());
	      props.put("group.id", tconf.getEsConsumerGroup());
	      props.put("enable.auto.commit", tconf.getAutoCommit()); 
	      props.put("auto.commit.interval.ms", tconf.getAutoCommitInterval());
	      props.put("session.timeout.ms", tconf.getSessionTimeout());
	      props.put("auto.offset.reset", tconf.getAutoOffsetReset());
	      
	      //要发送自定义对象，需要指定对象的反序列化类
	      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	      props.put("value.deserializer", tconf.getVSerialClass());
	      
	      consumer = new KafkaConsumer<String, String>(props);
	      consumer.subscribe(Arrays.asList(tconf.getTopic()));
	      taskQueue = new LinkedBlockingQueue<Runnable>(tconf.getMaxQueueSize());
	      workers = new ThreadPoolExecutor(tconf.getCoreSize(),
	    		  tconf.getMaxSize(),
                  30000,
                  TimeUnit.MILLISECONDS,
                  taskQueue,
                  new BulkThreadFactory(),
                  new KafkaRejectedExecutionHandler());
	}
	
    class BulkThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        BulkThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                                  Thread.currentThread().getThreadGroup();
            namePrefix = "bulk-pool-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                                  namePrefix + threadNumber.getAndIncrement(),
                                  0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }
	
	
	@Override
	public void run() {
		logger.info(Thread.currentThread().getName() +" poll handler thread start ...");
		
			//已消费条数，注意long值得上限
			long consumeCount=0L;
			//状态位
			boolean sPrepared=false;
			List<String> data = null;
			ConsumerRecords<String, String> records = null;
			while(true){
				try{
					/*pollLock.lock();
					try{
						while(taskQueue.size() == tconf.getMaxQueueSize()){
							notFull.await();
						}
						records = consumer.poll(200);
					}finally{
						pollLock.unlock();
					}*/
					
					//采用自旋方式
					while(taskQueue.size() > 150 || tconf.getSuspendSignal()){
						//idle
					}
					records = consumer.poll(200);
					
					if(records.count()==0 && consumeCount>0 && consumeCount<BULK_SIZE){
						workers.submit(new BulkRequestHandler(client,data,tconf,pollLock,notFull,taskQueue));
						consumeCount=0;
						sPrepared=false;
					}
					
					if(records.count()==0)
						continue;
					if(!sPrepared){
						data = new ArrayList<String>();
						sPrepared=true;
					}
					
					for(ConsumerRecord<String, String> r : records){
						data.add(r.value());
					}
					
					consumeCount=consumeCount+records.count();
					if(consumeCount>BULK_SIZE){
						workers.submit(new BulkRequestHandler(client,data,tconf,pollLock,notFull,taskQueue));
						logger.info("taskQueue current size="+taskQueue.size());
						sPrepared = false;
						consumeCount = 0;
					}
				}catch(Exception e){
					logger.info("error",e);
					logger.error("poll handler thread end,exception case",e);
				}
			}
		
	}

}
