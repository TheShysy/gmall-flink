package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/*
 *
 *@Author:shy
 *@Date:2020/12/21 14:28
 *
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdCountByProvince {
    private String province;
    private String windowEnd;
    private Long count;
}
