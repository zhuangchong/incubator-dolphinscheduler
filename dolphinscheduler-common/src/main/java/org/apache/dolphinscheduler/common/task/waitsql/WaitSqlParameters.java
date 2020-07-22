package org.apache.dolphinscheduler.common.task.waitsql;

import org.apache.commons.lang.StringUtils;
import org.apache.dolphinscheduler.common.process.ResourceInfo;
import org.apache.dolphinscheduler.common.task.AbstractParameters;

import java.util.ArrayList;
import java.util.List;
/**
 * Wait_Sql parameter
 */
public class WaitSqlParameters  extends AbstractParameters {
    /**
     * data source type，eg  MYSQL, POSTGRES, HIVE ...
     */
    private String type;

    /**
     * datasource id
     */
    private int datasource;

    /**
     * sql
     */
    private String sql;

    /**
     * udf list
     */
    private String udfs;

    /**
     * SQL connection parameters
     */
    private String connParams;
    /**
     * WAIT SQL look interval
     */
    private int lookInterval;



    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getDatasource() {
        return datasource;
    }

    public void setDatasource(int datasource) {
        this.datasource = datasource;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getUdfs() {
        return udfs;
    }

    public void setUdfs(String udfs) {
        this.udfs = udfs;
    }

    public int getLookInterval() {
        return lookInterval;
    }

    public void setLookInterval(int lookInterval) {
        this.lookInterval = lookInterval;
    }

    public String getConnParams() {
        return connParams;
    }

    public void setConnParams(String connParams) {
        this.connParams = connParams;
    }



    @Override
    public boolean checkParameters() {
        return datasource != 0 && StringUtils.isNotEmpty(type) && StringUtils.isNotEmpty(sql) && lookInterval!=0;
    }

    @Override
    public List<ResourceInfo> getResourceFilesList() {
        return new ArrayList<>();
    }

    @Override
    public String toString() {
        return "WaitSqlParameters{" +
                "type='" + type + '\'' +
                ", datasource=" + datasource +
                ", sql='" + sql + '\'' +
                ", udfs='" + udfs + '\'' +
                ", connParams='" + connParams + '\'' +
                ", lookInterval=" + lookInterval +
                '}';
    }
}