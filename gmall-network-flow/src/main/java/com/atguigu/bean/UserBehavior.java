package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/*
 *
 *@Author:shy
 *@Date:2020/12/19 16:40
 *
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior {

    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;

}
