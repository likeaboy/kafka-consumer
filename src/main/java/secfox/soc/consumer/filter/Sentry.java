package secfox.soc.consumer.filter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import secfox.soc.consumer.TConsumerConfig;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class Sentry {

	private final static Logger logger = LoggerFactory.getLogger(Sentry.class);
	
	private static boolean isCheck = false;
	private static long boundary = 0L;
	
	private static String antecedentIndex = null;
	private static String subsequentIndex = null;
	
	private static long scanStartTime = 0L;
	private static long scanEndTime = 0L;
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static String format(long time){
		return sdf.format(new Date(time));
	}
	
	public static void setScanStartTime(long scanStartTime) {
		Sentry.scanStartTime = scanStartTime;
	}

	public static void setScanEndTime(long scanEndTime) {
		Sentry.scanEndTime = scanEndTime;
	}

	public static long getScanStartTime() {
		return scanStartTime;
	}

	public static long getScanEndTime() {
		return scanEndTime;
	}

	public static void enableCheck(){
		isCheck = true;
		logger.info("[enable check,isCheck={}]",isCheck);
		TConsumerConfig tconf = TConsumerConfig.getInstance();
		//skyeye-las_event-yyyy.MM.dd
		antecedentIndex = tconf.getEsIndex();
		try {
			subsequentIndex = "skyeye-las_event-" + generateSubsequentIndex(antecedentIndex);
		} catch (ParseException e) {
			logger.error("",e);
			antecedentIndex = subsequentIndex;
		}
	}
	
	public static void disableCheck(){
		isCheck = false;
		logger.info("[disable check,isCheck={}]",isCheck);
	}
	
	public static boolean isCheck(){
		return isCheck;
	}
	
	/**
	 * update tconf es index when finished scan 
	 */
	public static void updateTconfEsIndex(){
		TConsumerConfig.getInstance().refreshIndex(subsequentIndex);
		logger.info("[updateTconfEsIndex to refresh index,use index={}]" + subsequentIndex);
	}
	
	/**
	 * 扫描结束后执行
	 */
	public static void updateBoundary(){
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		Date execDate = calendar.getTime();
		// execDate 比当前时间小，则取下一天的凌晨
		if (execDate.before(new Date())) {
			execDate = addDay(execDate, 1);
		}
		long old = boundary;
		boundary = execDate.getTime();
		logger.info("[do updateBoundary,boundary from : {} --> {}]",old,boundary);
	}
	
	public static TagAfterPeek peek(List<String> data){
		String head = data.get(0);
		
		if(!isAntecedent(getReceptTime(head))) return new TagAfterPeek(subsequentIndex,data);
		
		String tail = data.get(data.size()-1);
		
		if(isAntecedent(getReceptTime(tail))) return new TagAfterPeek(antecedentIndex,data);
		else{
			//head is antecedent,tail is subsequent,now find the slice point
			//start binary search range to find the slice point
			int[] range = binarySearchRange(data,0,data.size()-1);
			logger.info("[sentry do peek , range is : [{}~{}]]",range[0],range[1]);
			String[] original = new String[data.size()];
			original = data.toArray(original);
			int slicePointer = scan(data,range);
			
			String[] antecedentData = Arrays.copyOfRange(original, 0, slicePointer-1);
			String[] subsequentData = null;
			if(slicePointer != data.size())
				subsequentData = Arrays.copyOfRange(original, slicePointer-1,data.size());
			
			Map<String,String[]> slicingMap = new HashMap<String,String[]>(4);
			slicingMap.put(antecedentIndex, antecedentData);
			slicingMap.put(subsequentIndex, subsequentData);
			
			return new TagAfterPeek(slicingMap,true);
		}
	}
	
	public static String getAntecedentIndex() {
		return antecedentIndex;
	}

	public static String getSubsequentIndex() {
		return subsequentIndex;
	}

	private static String generateSubsequentIndex(String antecedentIndex) throws ParseException{
		String n = antecedentIndex.substring(antecedentIndex.length()-10);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd");
		Calendar c = Calendar.getInstance();
		c.setTime(sdf.parse(n));
		c.add(Calendar.DAY_OF_MONTH, 1);
		return sdf.format(c.getTime());
	}
	
	private static Date addDay(Date date, int num) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.DAY_OF_MONTH, num);
		return c.getTime();
	}
	
	private static int scan(List<String> data,int[] range){
		for(int i=range[0];i<=range[1];i++){
			if(!isAntecedent(getReceptTime(data.get(i))))return i;
		}
		//if not find, slice pointer = tail
		return data.size();
	}
	
	private static int[] binarySearchRange(List<String> data,int left,int right){
		if(right - left < 100) return new int[]{left,right};
		int mid = (left+right)/2;
		String json = data.get(mid);
		if(isAntecedent(getReceptTime(json))){
			left = mid;
			return binarySearchRange(data,left,right);
		}else{
			right = mid;
			return binarySearchRange(data,left,right);
		}
	}
	
	public static long getReceptTime(String text){
		JSONObject json = (JSONObject)JSON.parse(text);
		long receptTime = 0L;
		try{
			receptTime = Long.parseLong(json.get("recept_time").toString());
		}catch(Exception e){
			logger.error("",e);
			receptTime = 0L;
		}
		return receptTime;
	}
	
	private static boolean isAntecedent(long receptime){
		if(receptime < boundary) return true;
		
		return false;
	}
	public static void main(String[] args) {
		boundary = 1514563200085L;
		System.out.println(isAntecedent(1514563200085L));
	}
}
