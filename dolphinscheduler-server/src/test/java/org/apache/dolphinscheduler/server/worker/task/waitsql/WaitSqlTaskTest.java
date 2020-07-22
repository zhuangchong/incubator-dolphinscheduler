package org.apache.dolphinscheduler.server.worker.task.waitsql;

import org.apache.dolphinscheduler.common.enums.DbType;
import org.apache.dolphinscheduler.dao.entity.DataSource;
import org.apache.dolphinscheduler.server.entity.SQLTaskExecutionContext;
import org.apache.dolphinscheduler.server.entity.TaskExecutionContext;
import org.apache.dolphinscheduler.server.worker.task.TaskProps;
import org.apache.dolphinscheduler.server.worker.task.sqoop.SqoopTaskTest;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.util.Date;

@RunWith(MockitoJUnitRunner.Silent.class)
public class WaitSqlTaskTest {

    private static final Logger logger = LoggerFactory.getLogger(SqoopTaskTest.class);

    private ProcessService processService;
    private ApplicationContext applicationContext;
    private TaskExecutionContext taskExecutionContext;
    private WaitSqlTask waitSqlTask;
    private static final String CONNECTION_PARAMS = "{\"address\":\"jdbc:mysql://127.0.0.1:3306\",\"database\":\"dolphinscheduler\",\"jdbcUrl\":\"jdbc:mysql://127.0.0.1/dolphinscheduler?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&allowMultiQueries=true\",\"other\":\"{\"serverTimezone\":\"UTF-8\"}\",\"user\":\"root\",\"password\":\"root\"}";


    @Before
    public void before() throws Exception {
        processService = Mockito.mock(ProcessService.class);
        Mockito.when(processService.findDataSourceById(2)).thenReturn(getDataSource());
        applicationContext = Mockito.mock(ApplicationContext.class);
        SpringApplicationContext springApplicationContext = new SpringApplicationContext();
        springApplicationContext.setApplicationContext(applicationContext);
        Mockito.when(applicationContext.getBean(ProcessService.class)).thenReturn(processService);

        TaskProps props = new TaskProps();
        props.setTaskAppId(String.valueOf(System.currentTimeMillis()));
        props.setTenantCode("1");
        props.setEnvFile(".dolphinscheduler_env.sh");
        props.setTaskStartTime(new Date());
        props.setTaskTimeout(0);

        props.setTaskParams(
                "{\"type\":\"MYSQL\",\"datasource\":2,\"sql\":\"select 1 as test from dual\",\"lookInterval\":1}");


        taskExecutionContext = Mockito.mock(TaskExecutionContext.class);
        Mockito.when(taskExecutionContext.getTaskParams()).thenReturn(props.getTaskParams());
        Mockito.when(taskExecutionContext.getExecutePath()).thenReturn("/tmp");
        Mockito.when(taskExecutionContext.getTaskAppId()).thenReturn("1");
        Mockito.when(taskExecutionContext.getTenantCode()).thenReturn("root");
        Mockito.when(taskExecutionContext.getStartTime()).thenReturn(new Date());
        Mockito.when(taskExecutionContext.getTaskTimeout()).thenReturn(10000);
        Mockito.when(taskExecutionContext.getLogPath()).thenReturn("/tmp/dx");

        SQLTaskExecutionContext sqlTaskExecutionContext = new SQLTaskExecutionContext();
        sqlTaskExecutionContext.setConnectionParams(CONNECTION_PARAMS);
        Mockito.when(taskExecutionContext.getSqlTaskExecutionContext()).thenReturn(sqlTaskExecutionContext);

        waitSqlTask = new WaitSqlTask(taskExecutionContext,logger);
        waitSqlTask.init();
    }



    private DataSource getDataSource() {
        DataSource dataSource = new DataSource();
        dataSource.setType(DbType.MYSQL);
        dataSource.setConnectionParams(CONNECTION_PARAMS);
        dataSource.setUserId(1);
        return dataSource;
    }

    @Test
    public void testGetParameters() {
        Assert.assertNotNull(waitSqlTask.getParameters());
    }

    /**
     * Method: init
     */
    @Test
    public void testInit(){
        try {
            waitSqlTask.init();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Method: handle()
     */
    @Test
    public void testHandle()
            throws Exception {
        try {
            waitSqlTask.handle();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}
