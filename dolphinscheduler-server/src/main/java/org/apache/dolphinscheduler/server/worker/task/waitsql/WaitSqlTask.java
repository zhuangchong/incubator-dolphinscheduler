package org.apache.dolphinscheduler.server.worker.task.waitsql;

import com.alibaba.fastjson.JSON;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.DbType;
import org.apache.dolphinscheduler.common.enums.TaskTimeoutStrategy;
import org.apache.dolphinscheduler.common.process.Property;
import org.apache.dolphinscheduler.common.task.AbstractParameters;
import org.apache.dolphinscheduler.common.task.sql.SqlBinds;
import org.apache.dolphinscheduler.common.task.waitsql.WaitSqlParameters;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.CollectionUtils;
import org.apache.dolphinscheduler.common.utils.CommonUtils;
import org.apache.dolphinscheduler.common.utils.ParameterUtils;
import org.apache.dolphinscheduler.dao.datasource.BaseDataSource;
import org.apache.dolphinscheduler.dao.datasource.DataSourceFactory;
import org.apache.dolphinscheduler.server.entity.SQLTaskExecutionContext;
import org.apache.dolphinscheduler.server.entity.TaskExecutionContext;
import org.apache.dolphinscheduler.server.utils.ParamUtils;
import org.apache.dolphinscheduler.server.utils.UDFUtils;
import org.apache.dolphinscheduler.server.worker.task.AbstractTask;
import org.slf4j.Logger;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.dolphinscheduler.common.Constants.*;
import static org.apache.dolphinscheduler.common.enums.DbType.HIVE;

/**
 * waitsql task
 */
public class WaitSqlTask extends AbstractTask {


    /**
     * taskExecutionContext
     */
    private TaskExecutionContext taskExecutionContext;

    /**
     *  waitsql parameters
     */
    private WaitSqlParameters waitSqlParameters;

    /**
     * base datasource
     */
    private BaseDataSource baseDataSource;

    /**
     * default query sql limit
     */
    private static final int LIMIT = 10000;

    public WaitSqlTask(TaskExecutionContext taskExecutionContext, Logger logger){
        super(taskExecutionContext,logger);
        this.taskExecutionContext = taskExecutionContext;
    }

    @Override
    public void init() throws Exception {
        logger.info("wait_sql task params {}", taskExecutionContext.getTaskParams());
        this.waitSqlParameters =
                JSON.parseObject(taskExecutionContext.getTaskParams(), WaitSqlParameters.class);
        if (!waitSqlParameters.checkParameters()) {
            throw new RuntimeException("wait_sql task params is not valid");
        }
    }

    @Override
    public void handle() throws Exception {
        // set the name of the current thread
        String threadLoggerInfoName = String.format(Constants.TASK_LOG_INFO_FORMAT, taskExecutionContext.getTaskAppId());
        Thread.currentThread().setName(threadLoggerInfoName);

        logger.info("Full sql parameters: {}", waitSqlParameters);

        try {
            SQLTaskExecutionContext sqlTaskExecutionContext = taskExecutionContext.getSqlTaskExecutionContext();
            // load class
            DataSourceFactory.loadClass(DbType.valueOf(waitSqlParameters.getType()));

            // get datasource
            baseDataSource = DataSourceFactory.getDatasource(DbType.valueOf(waitSqlParameters.getType()),
                    sqlTaskExecutionContext.getConnectionParams());

            // ready to execute SQL and parameter entity Map
            SqlBinds mainSqlBinds = getSqlAndSqlParamsMap(waitSqlParameters.getSql());

            List<String> createFuncs = UDFUtils.createFuncs(sqlTaskExecutionContext.getUdfFuncTenantCodeMap(),
                    logger);


            // execute sql task
            executeFuncAndSql(mainSqlBinds, createFuncs);

            setExitStatusCode(Constants.EXIT_CODE_SUCCESS);

        } catch (Exception e) {
            setExitStatusCode(Constants.EXIT_CODE_FAILURE);
            logger.error("sql task error", e);
            throw e;
        }

    }

    /**
     *  ready to execute SQL and parameter entity Map
     * @return
     */
    private SqlBinds getSqlAndSqlParamsMap(String sql) {
        Map<Integer, Property> sqlParamsMap =  new HashMap<>();
        StringBuilder sqlBuilder = new StringBuilder();

        // find process instance by task id


        Map<String, Property> paramsMap = ParamUtils.convert(ParamUtils.getUserDefParamsMap(taskExecutionContext.getDefinedParams()),
                taskExecutionContext.getDefinedParams(),
                waitSqlParameters.getLocalParametersMap(),
                CommandType.of(taskExecutionContext.getCmdTypeIfComplement()),
                taskExecutionContext.getScheduleTime());

        // spell SQL according to the final user-defined variable
        if(paramsMap == null){
            sqlBuilder.append(sql);
            return new SqlBinds(sqlBuilder.toString(), sqlParamsMap);
        }

        //new
        //replace variable TIME with $[YYYYmmddd...] in sql when history run job and batch complement job
        sql = ParameterUtils.replaceScheduleTime(sql, taskExecutionContext.getScheduleTime());
        // special characters need to be escaped, ${} needs to be escaped
        String rgex = "['\"]*\\$\\{(.*?)\\}['\"]*";
        setSqlParamsMap(sql, rgex, sqlParamsMap, paramsMap);

        // replace the ${} of the SQL statement with the Placeholder
        String formatSql = sql.replaceAll(rgex, "?");
        sqlBuilder.append(formatSql);

        // print repalce sql
        printReplacedSql(sql, formatSql, rgex, sqlParamsMap);
        return new SqlBinds(sqlBuilder.toString(), sqlParamsMap);
    }

    @Override
    public AbstractParameters getParameters() {
        return this.waitSqlParameters;
    }

    /**
     * execute function and sql
     * @param mainSqlBinds          main sql binds
     * @param createFuncs           create functions
     */
    public void executeFuncAndSql(SqlBinds mainSqlBinds,
                                  List<String> createFuncs){
        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet resultSet = null;
        boolean waitFlag = true;
        while(waitFlag){
            try {
                // if upload resource is HDFS and kerberos startup
                CommonUtils.loadKerberosConf();
                // create connection
                connection = createConnection();
                // create temp function
                if (CollectionUtils.isNotEmpty(createFuncs)) {
                    createTempFunction(connection,createFuncs);
                }
                stmt = prepareStatementAndBind(connection, mainSqlBinds);

                // query statements need to be convert to JsonArray and inserted into Alert to send
                resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    String waitValue = resultSet.getString(1);
                    logger.info("wait_sql task execute sql {} ,result {}", waitSqlParameters.getSql(),waitValue);
                    waitFlag =!"1".equals( waitValue);
                }
            } catch (Exception e) {
                logger.error("execute sql error",e);
                throw new RuntimeException("execute sql error");
            } finally {
                close(resultSet,stmt,connection);
            }

            if(waitFlag){
                long usedTime = (System.currentTimeMillis() - taskExecutionContext.getStartTime().getTime()) / 1000;
                long remainTime = taskExecutionContext.getTaskTimeout() - usedTime;

                if (remainTime < 0) {
                    throw new RuntimeException("wait_sql task execution time out");
                }
                ThreadUtils.sleep(waitSqlParameters.getLookInterval() * Constants.SEC_2_MINUTES_TIME_UNIT * Constants.SLEEP_TIME_MILLIS);
            }
        }
    }

    /**
     * create temp function
     *
     * @param connection connection
     * @param createFuncs createFuncs
     * @throws Exception
     */
    private void createTempFunction(Connection connection,
                                    List<String> createFuncs) throws Exception{
        try (Statement funcStmt = connection.createStatement()) {
            for (String createFunc : createFuncs) {
                logger.info("hive create function sql: {}", createFunc);
                funcStmt.execute(createFunc);
            }
        }
    }
    /**
     * create connection
     *
     * @return connection
     * @throws Exception
     */
    private Connection createConnection() throws Exception{
        // if hive , load connection params if exists
        Connection connection = null;
        if (HIVE == DbType.valueOf(waitSqlParameters.getType())) {
            Properties paramProp = new Properties();
            paramProp.setProperty(USER, baseDataSource.getUser());
            paramProp.setProperty(PASSWORD, baseDataSource.getPassword());
            Map<String, String> connParamMap = CollectionUtils.stringToMap(waitSqlParameters.getConnParams(),
                    SEMICOLON,
                    HIVE_CONF);
            paramProp.putAll(connParamMap);

            connection = DriverManager.getConnection(baseDataSource.getJdbcUrl(),
                    paramProp);
        }else{
            connection = DriverManager.getConnection(baseDataSource.getJdbcUrl(),
                    baseDataSource.getUser(),
                    baseDataSource.getPassword());
        }
        return connection;
    }

    /**
     *  close jdbc resource
     *
     * @param resultSet resultSet
     * @param pstmt pstmt
     * @param connection connection
     */
    private void close(ResultSet resultSet,
                       PreparedStatement pstmt,
                       Connection connection){
        if (resultSet != null){
            try {
                connection.close();
            } catch (SQLException e) {

            }
        }

        if (pstmt != null){
            try {
                connection.close();
            } catch (SQLException e) {

            }
        }

        if (connection != null){
            try {
                connection.close();
            } catch (SQLException e) {

            }
        }
    }

    /**
     * preparedStatement bind
     * @param connection
     * @param sqlBinds
     * @return
     * @throws Exception
     */
    private PreparedStatement prepareStatementAndBind(Connection connection, SqlBinds sqlBinds) throws Exception {
        // is the timeout set
        boolean timeoutFlag = TaskTimeoutStrategy.of(taskExecutionContext.getTaskTimeoutStrategy()) == TaskTimeoutStrategy.FAILED ||
                TaskTimeoutStrategy.of(taskExecutionContext.getTaskTimeoutStrategy()) == TaskTimeoutStrategy.WARNFAILED;
        PreparedStatement stmt = connection.prepareStatement(sqlBinds.getSql());
        if(timeoutFlag){
            stmt.setQueryTimeout(taskExecutionContext.getTaskTimeout());
        }
        Map<Integer, Property> params = sqlBinds.getParamsMap();
        if(params != null) {
            for (Map.Entry<Integer, Property> entry : params.entrySet()) {
                Property prop = entry.getValue();
                ParameterUtils.setInParameter(entry.getKey(), stmt, prop.getType(), prop.getValue());
            }
        }
        logger.info("prepare statement replace sql : {} ", stmt);
        return stmt;
    }

    /**
     * regular expressions match the contents between two specified strings
     * @param content           content
     * @param rgex              rgex
     * @param sqlParamsMap      sql params map
     * @param paramsPropsMap    params props map
     */
    public void setSqlParamsMap(String content, String rgex, Map<Integer,Property> sqlParamsMap, Map<String,Property> paramsPropsMap){
        Pattern pattern = Pattern.compile(rgex);
        Matcher m = pattern.matcher(content);
        int index = 1;
        while (m.find()) {

            String paramName = m.group(1);
            Property prop =  paramsPropsMap.get(paramName);

            sqlParamsMap.put(index,prop);
            index ++;
        }
    }

    /**
     * print replace sql
     * @param content       content
     * @param formatSql     format sql
     * @param rgex          rgex
     * @param sqlParamsMap  sql params map
     */
    public void printReplacedSql(String content, String formatSql,String rgex, Map<Integer,Property> sqlParamsMap){
        //parameter print style
        logger.info("after replace sql , preparing : {}" , formatSql);
        StringBuilder logPrint = new StringBuilder("replaced sql , parameters:");
        for(int i=1;i<=sqlParamsMap.size();i++){
            logPrint.append(sqlParamsMap.get(i).getValue()+"("+sqlParamsMap.get(i).getType()+")");
        }
        logger.info("Sql Params are {}", logPrint);
    }
}
