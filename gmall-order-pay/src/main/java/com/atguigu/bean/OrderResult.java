package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/*
 *
 *@Author:shy
 *@Date:2020/12/22 14:26
 *
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderResult {
    private Long orderId;
    private String eventType;
}
