package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 将字符串转为大写字符串
 */
@Description(name = "up",
        value = "_FUNC_(object) - 将字符串转大写字符串")
public class Up extends UDF {
    public String evaluate(Object key) {
        if(null==key){
            return null;
        }
        return key.toString().toUpperCase();
    }
}
