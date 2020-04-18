package com.csw.ct.producer.bean;

import com.csw.ct.common.bean.Data;

/**
 * 联系人
 */
public class Contact extends Data {
    private String tel;
    private String name;

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setValue(Object val) {
        content = (String) val;
        String[] value = content.split ( "\t" );
        setName(value[1]);
        setTel(value[0]);
    }

    public String toString() {
        return "Contact["+tel+","+name+"]";
    }
}
