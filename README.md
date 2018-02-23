# kafka-consumer

kafka消息队列消费者

采用一个poll线程多个worker线程，消费者轮询kafka消息队列拉取数据，将数据封装为任务提交给worker线程池，多个线程并发写es；
