package hbase.zhibo8.testMybatis.service;


import com.zhibo8.testMybatis.entity.User;

import java.util.List;

public interface UserService {
    public List<User> getUserInfo();
    
    public void insert(User user) throws Exception;
}
