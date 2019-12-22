package com.javadeveloperzone.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Created by Lenovo on 19-07-2017.
 */
@Controller
public class SpringBootJSPExampleController {

    @GetMapping("/index")                     // it will handle all request for /welcome
    public String SpringBootHello() {
        return "index";           // welcome is view name. It will call index.jsp
    }
}
