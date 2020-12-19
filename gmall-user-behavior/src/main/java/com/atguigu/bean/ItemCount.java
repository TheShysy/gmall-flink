package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/*
 *
 *@Author:shy
 *@Date:2020/12/19 11:21
 *
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ItemCount {
    private Long itemId;
    private Long windowEnd;
    private Long count;
}
