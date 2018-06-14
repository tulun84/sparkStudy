package hbase.zhibo8.HbaseNote.com.itcast.tsc.login.service.impl;


import com.zhibo8.HbaseNote.com.itcast.tsc.login.dao.LoginDao;
import com.zhibo8.HbaseNote.com.itcast.tsc.login.service.LoginService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Service
public class LoginServiceImpl implements LoginService {

	@Resource(name="loginDaoImpl")
	private LoginDao loginDao;
	
	@Override
	public boolean login(String userName, String password) throws Exception{
		return loginDao.getLoginInfo(userName,password);
		
	}
}
