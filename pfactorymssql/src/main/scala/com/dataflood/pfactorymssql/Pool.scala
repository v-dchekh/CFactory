package com.dataflood.pfactorymssql

import java.sql.DriverManager
import java.sql.Connection
import org.apache.commons.pool2.PooledObject
import org.apache.commons.pool2.BasePooledObjectFactory
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.log4j.Logger

object Pool {
  protected val logger = Logger.getLogger(getClass.getName)

  def getSQLConnectPool(maxNumCon: Int) = {
    val p = createSQLConnectPool(Config.getСonnectionStringMSSQL(), maxNumCon)
    logger.info(p.getNumIdle, p.getNumWaiters, p.getNumActive)
    p
  }

  class BaseSQLConnectFactory(url: String) extends Serializable {
    def newInstance() = DriverManager.getConnection(url)
  }

  class PooledSQLConnectFactory(val factory: BaseSQLConnectFactory) extends BasePooledObjectFactory[Connection] with Serializable {
    override def create(): Connection = factory.newInstance()

    override def wrap(obj: Connection): PooledObject[Connection] = new DefaultPooledObject(obj)

    // From the Commons Pool docs: "Invoked on every instance when it is being "dropped" from the pool.  There is no
    // guarantee that the instance being destroyed will be considered active, passive or in a generally consistent state."
    override def destroyObject(p: PooledObject[Connection]): Unit = {
      p.getObject.close()
      super.destroyObject(p)
    }
  }
  def createSQLConnectPool(url: String, maxNumCon: Int): GenericObjectPool[Connection] = {
    val sqlConnectFactory = new BaseSQLConnectFactory(url)

    val pooledSQLConnectFactory = new PooledSQLConnectFactory(sqlConnectFactory)
    val poolConfig = {
      val c = new GenericObjectPoolConfig
      c.setMaxTotal(maxNumCon)
      c.setMaxIdle(maxNumCon)
      c
    }
    new GenericObjectPool[Connection](pooledSQLConnectFactory, poolConfig)

  }

}