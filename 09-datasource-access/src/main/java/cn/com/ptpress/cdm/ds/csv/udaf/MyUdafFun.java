package cn.com.ptpress.cdm.ds.csv.udaf;

import java.util.ArrayList;
import java.util.List;

/**
 * Example of a user-defined aggregate function (UDAF).
 */
import java.util.ArrayList;
import java.util.List;

public class MyUdafFun {

    public List<String> init() {
        System.out.println("udaf初始化...");
        return new ArrayList<>();
    }

    public List<String> add(List<String> accumulator, String v) {
        System.out.println("udaf添加数据...");
        accumulator.add(v);
        return accumulator;
    }

    public List<String> merge(List<String> accumulator1, List<String> accumulator2) {
        System.out.println("udaf合并数据...");
        accumulator1.addAll(accumulator2);
        return accumulator1;
    }

    public List<String> result(List<String> accumulator) {
        System.out.println("udaf返回数据...");
        return accumulator;
    }
}
