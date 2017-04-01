package org.jcy.utils;


import java.beans.PropertyVetoException;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;


import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DbcUtils {
	//初始化主库链接
    private static ComboPooledDataSource  db1 = new ComboPooledDataSource();
    //初始化副库链接
    private static ComboPooledDataSource  db2 = new ComboPooledDataSource();
    //初始化properties文件流
    private static Properties props = new Properties();
    static {
    	Readervalue();
    	try {
    		//获取主库配置信息
			db1.setDriverClass(props.getProperty("driverClass").toString());
			db1.setJdbcUrl(props.getProperty("jdbcUrl").toString());
			db1.setUser(props.getProperty("user").toString());
			db1.setPassword(props.getProperty("password").toString());
			db1.setAcquireIncrement(1000);
			db1.setInitialPoolSize(2);
			db1.setMinPoolSize(2);
			db1.setMaxPoolSize(20);
			db1.setMaxIdleTime(250);
			//获取副库配置信息
			db2.setDriverClass(props.getProperty("driverClass1").toString());
			db2.setJdbcUrl(props.getProperty("jdbcUrl1").toString());
			db2.setUser(props.getProperty("user1").toString());
			db2.setPassword(props.getProperty("password1").toString());
			db2.setAcquireIncrement(1000);
			db2.setInitialPoolSize(2);
			db2.setMinPoolSize(2);
			db2.setMaxPoolSize(20);
			db2.setMaxIdleTime(250);
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
    }
    public static DataSource getDataSource1() {
    	return db1;
    }
    public static DataSource getDataSource2() {
    	return db2;
    }
    
    public static Connection getConnection1() {
    	try {
			return db1.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
    	return null;
    }
    public static Connection getConnection2() {
    	try {
			return db2.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
    	return null;
    }
    private static void Readervalue(){
    	FileInputStream fis = null;
	        try {
	        	fis = new FileInputStream("datasource.properties");
	        	props.load(fis);
	        } catch (IOException e) {
	    } 
	}
}