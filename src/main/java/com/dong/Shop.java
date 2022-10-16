package com.dong;

import lombok.Data;
import java.io.Serializable;

@Data
public class Shop implements Serializable {
    public String shopName;
    public Integer shopID;
}
