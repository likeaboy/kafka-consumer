# kafka-consumer

kafka消息队列消费者

采用一个poll线程多个worker线程，worker负责调用es bulk api（同步阻塞）批量写es，
消费者轮询kafka消息队列拉取数据，将数据封装为任务提交给worker线程池，多个线程并发写es。

编译部署启动：
采用maven编译发布，mvn install后kafka-consumer-bin.zip，将zip解压到指定部署路径，执行startup.sh脚本启动。
