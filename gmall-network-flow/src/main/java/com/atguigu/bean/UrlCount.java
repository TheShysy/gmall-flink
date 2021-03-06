package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/*
 *
 *@Author:shy
 *@Date:2020/12/19 14:15
 *
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UrlCount {
    private String url;
    private Long windowEnd;
    private Long count;
}