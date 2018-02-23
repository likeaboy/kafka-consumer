package secfox.soc.consumer.mutil;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import secfox.soc.consumer.TConsumerConfig;
import secfox.soc.consumer.filter.Sentry;
import secfox.soc.consumer.filter.TagAfterPeek;
/**
 * 批量提交es
 * @author wangzhijie
 *
 */
public class BulkRequestHandler implements Runnable{
	
	private final static Logger logger = LoggerFactory.getLogger(BulkRequestHandler.class);
	
	private TransportClient client;
	private List<String> data;
	private TConsumerConfig tconf;
	private LinkedBlockingQueue<Runnable> taskQueue;
	
	public BulkRequestHandler(TransportClient client,List<String> data,TConsumerConfig tconf,ReentrantLock pollLock,Condition notFull,LinkedBlockingQueue<Runnable> taskQueue){
		this.client = client;
		this.data = data;
		this.tconf = tconf;
		this.taskQueue = taskQueue;
	}
	
	public List<String> getData(){
		return data;
	}
	

	@Override
	public void run() {
		try{
			run0(tconf.getEsIndex(),this.data);
			/*if(Sentry.isCheck()) run0(tconf.getEsIndex(),this.data);
			else run1();*/
		}catch(Exception e){
			//batch commit error，暂时不处理
			logger.error("batch commit error",e);
			//可以考虑开启异步线程，将error数据回补到es，成本过高
			//考虑到batch commit error的情况，即所有worker线程全部失败，所以考虑阻塞poll thread来达到
			//写es丢数据问题，并将当前任务塞回blocking queue
			taskQueue.add(this);
			tconf.doSuspend();
			logger.info("[do suspend poll thread, status(isSuspendSignal) is : "+tconf.getSuspendSignal()+"]");
		}
	}
	
	private void run0(String esIndex,List<String> toHandleData) {
		long start = System.currentTimeMillis();
		int failCount = 0;
		BulkRequestBuilder bulkRequest=this.client.prepareBulk();
		IndexRequestBuilder idxBuilder = this.client.prepareIndex(esIndex,tconf.getEsType());
		for(String json: toHandleData){
			bulkRequest.add(idxBuilder.setSource(json));
		}
		long start0 = System.currentTimeMillis();
		
		//同步阻塞
		BulkResponse bulkResp = bulkRequest.execute().actionGet();
		long end = System.currentTimeMillis();
		BulkItemResponse[] responses = bulkResp.getItems();
		for (int i = 0; i < responses.length; i++) {
			BulkItemResponse response = responses[i];
			 if (response.isFailed()) failCount++;
		}
		StringBuilder respResultMsg = new StringBuilder();
		respResultMsg.append(" [took cost :"+bulkResp.getTookInMillis()+"ms] [total commit message="+responses.length + "] [fail commit message="+failCount + "]");
		
		logger.info(Thread.currentThread().getName() + " [bulk worker do commit success,dsize="+toHandleData.size() + "] [total cost:"+(end-start)+"ms]" + " [sync cost:"+(end-start0)+"ms]");
		logger.info(Thread.currentThread().getName() + respResultMsg.toString());
	}
	
	private void run1(){
		TagAfterPeek tag = Sentry.peek(this.data);
		
		if(tag.isSlacing()){
			Map<String,String[]> slicingMap = tag.getSlicingMap();
			if(slicingMap.get(Sentry.getAntecedentIndex())!=null){
				run0(Sentry.getAntecedentIndex(),Arrays.asList(slicingMap.get(Sentry.getAntecedentIndex())));
			}
			if(slicingMap.get(Sentry.getSubsequentIndex())!=null){
				run0(Sentry.getSubsequentIndex(), Arrays.asList(slicingMap.get(Sentry.getSubsequentIndex())));
			}
		}else{
			run0(tag.getIndex(),tag.getData());
		}
		
	}

}
