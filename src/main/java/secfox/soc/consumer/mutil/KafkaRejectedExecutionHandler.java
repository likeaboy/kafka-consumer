package secfox.soc.consumer.mutil;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaRejectedExecutionHandler implements RejectedExecutionHandler{
	
	private final static Logger logger = LoggerFactory.getLogger(KafkaRejectedExecutionHandler.class);
	private AtomicInteger discardReqCount = new AtomicInteger(0);

	@Override
	public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
		// TODO Auto-generated method stub
		discardReqCount.getAndIncrement();
		logger.info("[ bulk commit error, because rejected , discard reqsize=" + discardReqCount + " ]");
	}

}
