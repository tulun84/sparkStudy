package structuredStreaming.sink.util

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.dbcp.BasicDataSource

/**
  *  * @Description: DataSourcePool DBCP 连接池
  *  * @Author: yang.yang   yang.yang03@luckincoffee.com
  *  * @Date: 2019/4/22 16:23
  *  */

object DataSourcePool {

  @volatile var dataSourceMap:  scala.collection.mutable.Map[String, BasicDataSource] =  scala.collection.mutable.Map()

  /**
    * 获取数据源
    * @param mysqlConfig 数据源配置
    * @return 创建好的数据源
    */
  def getDataSource(mysqlConfig: String): BasicDataSource = {
    dataSourceMap.synchronized {
      val dataSource: BasicDataSource = dataSourceMap.get(mysqlConfig).orNull
      if (dataSource == null) {
        val newDataSource = createDataSource(mysqlConfig)
        dataSourceMap += (mysqlConfig->newDataSource)
        newDataSource
      } else dataSource
    }
  }

  def createDataSource(mysqlConfig: String): BasicDataSource = {
    val mysqlObject: JSONObject = JSON.parseObject(mysqlConfig)
    val userName: String = mysqlObject.getString("username")
    val password: String = mysqlObject.getString("password")
    val driverClassName: String = mysqlObject.getString("driverClassName")
    val url: String = mysqlObject.getString("url")
    val maxIdle: Int = mysqlObject.getString("maxIdle").toInt
    val minIdle: Int = mysqlObject.getString("minIdle").toInt
    val maxWait: Int = mysqlObject.getString("maxWait").toInt
    val maxActive: Int = mysqlObject.getString("maxActive").toInt
    val initialSize: Int = mysqlObject.getString("initialSize").toInt
    val decryptPassword: String = DatabaseEncryption.decrypt(password)
    val dataSource: BasicDataSource = new BasicDataSource
    dataSource.setUsername(userName)
    dataSource.setPassword(decryptPassword)
    dataSource.setUrl(url)
    dataSource.setDriverClassName(driverClassName)
    dataSource.setPoolPreparedStatements(true)
    //最大空闲数
    dataSource.setMaxIdle(maxIdle)
    //最小空闲数
    dataSource.setMinIdle(minIdle)
    //最长等待毫秒值
    dataSource.setMaxWait(maxWait)
    //最大链接数量
    dataSource.setMaxActive(maxActive)
    //连接池初始化连接数
    dataSource.setInitialSize(initialSize)
    // dataSourceMap += (mysqlConfig -> dataSource)
    dataSource
  }
}
