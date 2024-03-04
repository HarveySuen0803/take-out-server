package com.hmdp.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;

@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@TableName("tb_customer")
public class Customer implements Serializable {
    @TableId(value = "id", type = IdType.AUTO)
    private Integer id;
    
    private String cname;
    
    private Integer age;
    
    private String phone;
    
    private byte sex;
    
    private static final long serialVersionUID = 1L;
}
