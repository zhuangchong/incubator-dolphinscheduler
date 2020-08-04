# 1.开发

## 官网开发指南
https://dolphinscheduler.apache.org/zh-cn/docs/development/subscribe.html



## pom
    
* parent model:pom.xml 去除mysql scope=test
```xml parent model:pom.xml
            <dependency>
                <groupId>mysql</groupId>
                <artifactId>mysql-connector-java</artifactId>
                <version>${mysql.connector.version}</version>
            </dependency>
          
```
* parent model:pom.xml & dao model:pom.xml 增加 oracle 
```xml parent model:pom.xml
            <dependency>
                <groupId>com.oracle.ojdbc</groupId>
                <artifactId>ojdbc8</artifactId>
                <version>${oracle.driver.version}</version>
            </dependency>
```
```xml dao model:pom.xml
        <dependency>
			<groupId>com.oracle.ojdbc</groupId>
			<artifactId>ojdbc8</artifactId>
		</dependency>
```

---
CDH jar 替换
* <hadoop.version>3.0.0-cdh6.0.0</hadoop.version>
* <hive.jdbc.version>2.1.1-cdh6.0.0</hive.jdbc.version>
* <repositories>
         <repository>
             <id>cloudera-repository</id>
             <name>cloudera repository</name>
             <url>http://repository.cloudera.com/artifactory/cloudera-repos/</url>
         </repository>
         <repository>
             <id>aliyun-repository</id>
             <name>aliyun repository</name>
             <url>http://maven.aliyun.com/nexus/content/groups/public/</url>
         </repository>
     </repositories>

---
## jar 漏洞

* fastjson =1.2.72

---

---      


# 2.升级上线
# 编译
* shell 脚本使用UE编译unix格式(第一次)
---
* ui 多次 npm install(第一次)
---
* 打包
    * mvn -U clean package -Prelease -Dmaven.test.skip=true
    * 二进制包路径：dolphinscheduler-dist/target

---



# 3.功能

## 去除sudo,每个租户下一个worker
* TODO 去除sudo,各租户启动worker-server  【不需要修改】
    * AbstractCommandExecutor
    * FileUtils
    * ProcessUtils
    * TaskKillProcessor
    
    已修改，sudo可支持【普通用户 sudo 免密到 普通用户】
    ```shell script
     >vi /etc/sudoers
     stat_dispatch_phq  ALL=(stat_nocar_phq,stat_comm_phq) NOPASSWD:ALL
    ```
---
## 新增等表插件
* org.apache.dolphinscheduler.server.worker.task.waitsql.WaitSqlTask
* org.apache.dolphinscheduler.common.task.waitsql.WaitSqlParameters
* org.apache.dolphinscheduler.common.enums.TaskType
* org.apache.dolphinscheduler.server.worker.task.TaskManager
* org.apache.dolphinscheduler.common.utils.TaskParametersUtils
* org.apache.dolphinscheduler.server.master.consumer.TaskPriorityQueueConsumer

* formModel.vue
* toolbar_WAIT_SQL.png
* wait_sql.vue
* timeoutAlarm.vue
* config.js
* dag.scss
* en_US.js
* zh_CN.js


---
