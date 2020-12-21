package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/*
 *
 *@Author:shy
 *@Date:2020/12/19 14:16
 *
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApacheLog {
    private String ip;
    private String userId;
    private Long eventTime;
    private String method;
    private String url;
}
