package com.tulun.sparkF;

import java.io.File;

/**
 * 执行方法：
 * （注意：由于打包时没有指定main主类，所以不能用 -jar方式，要用 -cp方式指定主类）
 * java -cp spark/original-MySpark-1.0-SNAPSHOT.jar com.tulun.sparkF.Main script/aaa.sql -date "2013-01-01"
 */

public class Main {

	/**
	 * @param    /*.sql  -date "2013-01-01"  -date1 2013-01-01
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
//		args = new String[3];
//		args[0]="src/main/testFiles/daily_visit.sql";
//		args[1]="-date";
//		args[2]="2013-01-01";

		//首个参数以外的其他参数，存到Map里
		ParseArgs parse = new ParseArgs(args);
		// 把sql文件转成一个sql字符串
		String sql = Utils.getSql(new File(args[0])) ;
		// 把sql字符串进行处理，把里面出现map的key的地方用value替换
		String str=Utils.parse(sql,parse.getMap());
		System.out.println(str);
	}

}
