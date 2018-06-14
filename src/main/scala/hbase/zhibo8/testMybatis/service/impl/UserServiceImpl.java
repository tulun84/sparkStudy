package hbase.zhibo8.testMybatis.service.impl;


import com.zhibo8.testMybatis.entity.User;
import com.zhibo8.testMybatis.mapper.UserMapper;
import com.zhibo8.testMybatis.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;


@Service
public class UserServiceImpl implements UserService {

	@Autowired
    private UserMapper userMapper;

    public List<User> getUserInfo(){
        return userMapper.findUserInfo();
    }

    //@Transactional开启事务
    @Transactional
	public void insert(User user) throws Exception{

		userMapper.addUserInfo(user);
		int i=1/0;
		userMapper.addUserInfo(user);
		
	}
}
