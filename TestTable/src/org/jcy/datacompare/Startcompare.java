package org.jcy.datacompare;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.jcy.utils.DbcUtils;

public class Startcompare {
    private static Properties props = new Properties();
	private static boolean queryclum = false;
	private static ArrayList<String> clumlist =  new ArrayList<>();
	private static List<String> lesstable =  new ArrayList<>();
	private static String database;
	private static String databasename1;
	private static String databasename2;
	private static String path;
	static {
		Readervalue();
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd_HHmmss");//设置日期格式
		path = df.format(new Date())+".txt";
	}
	public static void main(String[] args){
		//表 白名单
		List<String> datalist = new ArrayList<>();
		String[] datastr = props.getProperty("tables").split(",");
		if(props.getProperty("tables")!=null&&!"".equals(props.getProperty("tables").toString())){
			datalist = Arrays.asList(datastr);
			selectTabledata(datalist);
		}else{
			String sql1 = null;
			String sql2 = null;
			if(database.equals("oracle")||database.equals("dm")){
				//oracle/dm查询表名
				sql1 = "SELECT TABLE_NAME FROM USER_TABLES";
				sql2 = "SELECT TABLE_NAME FROM USER_TABLES";
			} else if(database.equals("mysql")){
				//mysql根据数据库名 查询表名
				sql1 = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '"+databasename1+"'";
				sql2 = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '"+databasename2+"'";
			} else {
				System.err.println("database.properties配置错误");
				System.exit(0);
			}
			List<String> sqltable1 = exicute1(sql1);
			List<String> sqltable2 = exicute2(sql2);
			//获取主库比副库多得表
			lesstable = getDiffrent(sqltable1,sqltable2);
			//取主库与副库的交集
			sqltable1.retainAll(sqltable2);
			selectTabledata(sqltable1);
		}
		
	}
	private static void Readervalue(){
		FileInputStream fis = null;
        try {
        	fis = new FileInputStream("datasource.properties");
        	props.load(fis);
        	} catch (IOException e) {
        } 
			database = props.getProperty("database");
			databasename1 = props.getProperty("databasename1");
			databasename2 = props.getProperty("databasename2");
	}
	private static List<Map<String,String>> selectTabledata(List<String> list){
		String sql = null;
		String prikey = null;
		String tabledatasql = null;
		String querybyprikeysql = null;
		List<String> tabledata1 = new ArrayList<>();
		List<String> tabledata2 = new ArrayList<>();
		List<String> tabledata = new ArrayList<>();
		List<String> lessdata = new ArrayList<>();
		StringBuffer sbf = new StringBuffer();
		for(String str : list){
			//从list中获取表名str
			//查询表中的主键
			if(database.equals("oracle")||database.equals("dm")){
				//oracle/dm查询方式
				sql = "select a.column_name from user_cons_columns a, user_constraints b where a.constraint_name = b.constraint_name  and b.constraint_type = 'P'  and a.table_name ='"+str+"'";
			}else if(database.equals("mysql")){
				//mysql查询方式
				sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='"+str+"'";
			} else {
				System.err.println("database.properties配置错误");
				System.exit(0);
			}
			
			tabledata = exicute1(sql);
			//获取主键
			if(tabledata.size()!=0){
				prikey = tabledata.get(0);
				//根据主键分别查询表数据  并寻找A表比B表多的数据
				tabledatasql = "select " +prikey+ " from " +str;
				tabledata1 = exicute1(tabledatasql);
				tabledata2 = exicute2(tabledatasql);
				System.out.println("-->query table " + str);
				List<String> diflist = getDiffrent(tabledata1,tabledata2);
				
				//根据表名查询表中所有的列名
				if(diflist.size()!=0){
					sbf.append("-- --------------------\r\n-- Records of "+str+"\r\n-- --------------------\r\n");
					queryclum = true;
					for(String pristr : diflist){
						querybyprikeysql = "select * from " + str + " where " + prikey + "=" +"'"+pristr+"'";
						//根据主表比附表多的数据的主键，查询数据详情
						lessdata = exicute1(querybyprikeysql);
						sbf.append("insert into "+ str +"(");
						for(int i=0;i<clumlist.size();i++){
							sbf.append(clumlist.get(i)+",");
						}
						sbf.delete(sbf.length()-1, sbf.length());
						sbf.append(") values (");
						for(int i=0;i<lessdata.size();i++){
							if(lessdata.get(i)==null){
								sbf.append("null"+",");
							}else{
								sbf.append("'"+lessdata.get(i)+"'"+",");
							}
							
						}
						sbf.delete(sbf.length()-1, sbf.length());
						sbf.append(");\r\n");
						if(queryclum == true){
							queryclum = false;
						}
						
					}
					clumlist.clear();
				}
				writerFile(sbf.toString());
				sbf.delete(0, sbf.length());
			}
		}
		StringBuffer sbf1 = new StringBuffer();
		if(lesstable.size()!=0){
			for(int i = 0;i < lesstable.size();i++){
				sbf1.append(lesstable.get(i)+"\r\n");
			}
			sbf1.delete(lesstable.size()-1, lesstable.size());
			System.out.println("下表未参与对比:\r\n"+sbf1.toString());
		}
		System.out.println("对比结束");
		return null;
	}
	//查询
	private static List<String> exicute1(String sql) {
		Connection conn = DbcUtils.getConnection1();
		ResultSet rs = null;
		List<String> list = null;
		PreparedStatement pstmt = null;
		if (sql != null && !"".equals(sql)) {
			try {
				pstmt = conn.prepareStatement(sql);
				rs = pstmt.executeQuery();
				list = resultSetToList(rs);
			} catch (SQLException e) {
				e.printStackTrace();
			} finally{
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return list;
	}
	private static List<String> exicute2(String sql) {
		Connection conn = DbcUtils.getConnection2();
		ResultSet rs = null;
		List<String> list = null;
		PreparedStatement pstmt = null;
		if (sql != null && !"".equals(sql)) {
			try {
				pstmt = conn.prepareStatement(sql);
				rs = pstmt.executeQuery();
				list = resultSetToList(rs);
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return list;
	}

	private static List<String> resultSetToList(ResultSet rs) throws java.sql.SQLException {
		if (rs == null)
			return Collections.emptyList();
		ResultSetMetaData md = rs.getMetaData(); // 得到结果集(rs)的结构信息，比如字段数、字段名等
		int columnCount = md.getColumnCount(); // 返回此 ResultSet 对象中的列数
		List<String> list = new ArrayList<>();
		while (rs.next()) {
			for (int i = 1; i <= columnCount; i++) {
				if(queryclum==true){
					String a = md.getColumnName(i);
					clumlist.add(a);
				}
				if("".equals(rs.getObject(i))||null==rs.getObject(i)){
					list.add(null);
				}else{
					list.add(rs.getObject(i).toString());
				}
			}
		}
		return list;
	}
	 /** 
     * 获取List1中存在 list2不存在的元素 
     * @param list1 
     * @param list2 
     * @return 
     */ 
	 private static List<String> getDiffrent(List<String> list1, List<String> list2) {  
	         List<String> diff = new ArrayList<String>();  
	         List<String> maxList = list1;  
	         List<String> minList = list2;  
	         Map<String,Integer> map = new HashMap<String,Integer>(maxList.size());  
	         for (String string : minList) {  
	             map.put(string, 1);  
	         }  
	         for (String string : maxList) {  
	             if(map.get(string)==null)  
	             {  
	            	 diff.add(string);  
	             }  
	         }  
	        return diff;  
	    } 
	 //将脚本写入文档
	 private static void writerFile(String filedata){
	        try {
	        	File file = new File(path);
	        	if(!file.exists()){
	        		file.createNewFile();
	        	}
	            //打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
	            FileWriter writer = new FileWriter(file, true);
	            writer.write(filedata);
	            writer.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	 }
}
