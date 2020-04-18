package com.csw.ct.common.bean;

import java.io.Closeable;

/**
 * 生产者接口
 */
public interface Producer extends Closeable {

    public void setIn(Datain in);
    public void setOut(Dataout out);
    /**
     * 生产数据
     */
    public void produce();
}
