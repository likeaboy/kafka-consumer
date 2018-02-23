package secfox.soc.consumer.cron;

import java.util.Calendar;
import java.util.Date;

public class AbstractCronComponent implements IComponent{
	
	public Date addDay(Date date, int num) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.DAY_OF_MONTH, num);
		return c.getTime();
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		
	}

}
