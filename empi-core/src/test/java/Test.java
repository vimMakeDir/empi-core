import com.ksyun.empi.core.algorithm.DistanceMetric;
import com.ksyun.empi.core.algorithm.JaroWinklerAliasiDistanceMetric;
import com.ksyun.empi.core.algorithm.Impl.StringComparisonServiceImpl;

import java.util.HashMap;


public class Test {

    public static void main(String[] args) {
        String str = "宠物小精灵";
        String name = "1宠物";
//        JaroWinklerAliasiDistanceMetric jaroWinklerAliasiDistanceMetric = new JaroWinklerAliasiDistanceMetric();
//        System.out.println(jaroWinklerAliasiDistanceMetric.score(str, name));

        StringComparisonServiceImpl stringComparisonService = new StringComparisonServiceImpl();
        HashMap<String, DistanceMetric> metricTypeMap = new HashMap<>();
        DistanceMetric distanceMetric = new JaroWinklerAliasiDistanceMetric();
        metricTypeMap.put("JaroWinklerAliasiDistanceMetric", distanceMetric);
        stringComparisonService.setDistanceMetricTypeMap(metricTypeMap);
        System.out.println(stringComparisonService.score("JaroWinklerAliasiDistanceMetric", str, name));
    }
}
