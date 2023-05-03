package udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * 参考
 * org.apache.hadoop.hive.contrib.udaf.example.UDAFExampleAvg
 */
@Description(name = "avg_num",
        value = "_FUNC_(col) - 计算平均数")
public class AvgNum extends UDAF {

    public static class UDAFAvgState {
        private long mCount;
        private double mSum;
    }

    public static class UDAFExampleAvgEvaluator implements UDAFEvaluator {

        UDAFAvgState state;

        public UDAFExampleAvgEvaluator() {
            super();
            state = new UDAFAvgState();
            init();
        }

        public void init() {
            state.mSum = 0;
            state.mCount = 0;
        }

        /**
         * 遍历所有行
         * @param o
         * @return
         */
        public boolean iterate(Double o) {
            if (o != null) {
                state.mSum += o;
                state.mCount++;
            }
            return true;
        }

        /**
         *  输出部分聚合结果
         * @return
         */
        public UDAFAvgState terminatePartial() {
            // This is SQL standard - average of zero items should be null.
            return state.mCount == 0 ? null : state;
        }

        /**
         * 提前合并部分数据
         * @param o
         * @return
         */
        public boolean merge(UDAFAvgState o) {
            if (o != null) {
                state.mSum += o.mSum;
                state.mCount += o.mCount;
            }
            return true;
        }

        /**
         * 终止聚合并返回最终结果
         * @return
         */
        public Double terminate() {
            // This is SQL standard - average of zero items should be null.
            return state.mCount == 0 ? null : Double.valueOf(state.mSum
                    / state.mCount);
        }


    }



}
