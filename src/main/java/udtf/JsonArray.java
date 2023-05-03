package udtf;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import org.json.JSONArray;
import org.json.JSONException;
import java.util.ArrayList;

@Description(name = "json_array",
        value = "_FUNC_(array_string) - 将字符串转为集合对象")
public class JsonArray extends UDF {

    public ArrayList<String> evaluate(String jsonString) throws JSONException {

        if (StringUtils.isBlank(jsonString)) {
            return null;
        }

        JSONArray extractObject = new JSONArray(jsonString);
        ArrayList<String> result = new ArrayList<String>();
        for (int i = 0; i < extractObject.length(); i++) {
            result.add(extractObject.get(i).toString());
        }
        return result;

    }
}
