package com.tipdm.scala.utils

import java.io.{BufferedInputStream, InputStream}
import java.util.Properties

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * Created by ch on 2018/8/21
  */
object InternalRedisClient extends Serializable {
  @transient private var pool: JedisPool = null
  val properties:Properties = new Properties()
  val in:InputStream = getClass.getResourceAsStream("/sysconfig/redis.properties")
  properties.load(new BufferedInputStream(in))
  val redisHost= properties.getProperty("redis.host")
  val redisPort = properties.getOrDefault("redis.port","6379").toString.toInt
  val redisTimeout:Int = properties.getOrDefault("redis.timeout","3000").toString.toInt
  val redisPassword = properties.getProperty("redis.password")
  val redisDBindex= properties.getOrDefault("redis.db","8").toString.toInt
  val redisMaxIdle = properties.getOrDefault("redis.maxidle","10").toString.toInt
  val redisMinIdle = properties.getOrDefault("redis.minidle","2").toString.toInt
  val redisMaxTotal = properties.getOrDefault("redis.maxtotal","10").toString.toInt
  def getJedis(): Jedis ={
    if(pool==null){
      makePool(redisHost,redisPort,redisTimeout,redisPassword,redisDBindex,redisMaxTotal,redisMaxIdle,redisMinIdle)
    }
    pool.getResource
  }
  def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,password :String,database:Int,
               maxTotal: Int, maxIdle: Int, minIdle: Int): Unit = {
    makePool(redisHost, redisPort, redisTimeout, password,database,maxTotal, maxIdle, minIdle, true, false, 10000)
  }

  def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,password :String,database:Int,
               maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean,
               testOnReturn: Boolean, maxWaitMillis: Long): Unit = {
    if(pool == null) {
      val poolConfig = new GenericObjectPoolConfig()
      poolConfig.setMaxTotal(maxTotal)
      poolConfig.setMaxIdle(maxIdle)
      poolConfig.setMinIdle(minIdle)
      poolConfig.setTestOnBorrow(testOnBorrow)
      poolConfig.setTestOnReturn(testOnReturn)
      poolConfig.setMaxWaitMillis(maxWaitMillis)
      pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout,password,database)


      val hook = new Thread{
        override def run = pool.destroy()
      }
      sys.addShutdownHook(hook.run)
    }
  }

  def getPool: JedisPool = {
    if(pool==null){
      makePool(redisHost,redisPort,redisTimeout,redisPassword,redisDBindex,redisMaxTotal,redisMaxIdle,redisMinIdle)
    }
    pool
  }

}
