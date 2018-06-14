package hbase.zhibo8.testMybatis.controller;


import com.zhibo8.testMybatis.entity.User;
import com.zhibo8.testMybatis.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;


@RestController  
@RequestMapping("/user")  
public class UserController {  
	@Autowired
	private UserService userService;
	
	@RequestMapping("/getUserInfo")
    public List<User> getUserInfo() {
		List<User> user = userService.getUserInfo();
        System.out.println(user.toString());
        return user;
    }
	
	@RequestMapping("/addUserInfo")
    public String addUserInfo() {
		User user = new User();
		user.setId(3L);
		user.setName("cwh");
		try {
			userService.insert(user);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "success:"+user.toString();
    }
	
	
}  
