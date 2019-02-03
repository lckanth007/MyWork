val pool = new ThreadPoolExecutor(2, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable](1))
	
    for (i <- reqList.split(",").toList)
       {
    pool.execute(new Runnable {
      override def run(): Unit = {
          println(s"${Thread.currentThread().getName}: $i HAS STARTED !!!!")
          doUnionSql(i)
          println(s"${Thread.currentThread().getName}: $i IS COMPETED !!!!")
      }
    })
	Thread.sleep(60000)
  }
  
  while(pool.getActiveCount() != 0)
  {
    println("There is Thread Still Running !!!!")
    println("The Number of Threads running are "+pool.getActiveCount())
    Thread.sleep(10000)
  }
  
  println("ALL THE THREADS HAVE COMPLETED !!!!")
