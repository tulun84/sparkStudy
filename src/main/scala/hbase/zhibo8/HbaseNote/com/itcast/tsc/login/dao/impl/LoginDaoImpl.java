package hbase.zhibo8.HbaseNote.com.itcast.tsc.login.dao.impl;

import com.zhibo8.HbaseNote.com.itcast.tsc.login.dao.LoginDao;
import com.zhibo8.HbaseNote.com.itcast.tsc.util.RedisTools;
import com.zhibo8.HbaseNote.com.itcast.tsc.util.constants.Constants;
import org.springframework.stereotype.Service;


@Service("loginDaoImpl")
public class LoginDaoImpl implements LoginDao {

	@Override
	public boolean getLoginInfo(String userName, String password) throws Exception {
		boolean flag = false;
		String userInfo = RedisTools.get(userName);
		if (userInfo!=null) {
			String[] split = userInfo.split("\\"+ Constants.STRING_SEPARATOR);
			if (password.equals(split[0])) {
				flag=true;
			}
		}
		return flag;
	}

}
