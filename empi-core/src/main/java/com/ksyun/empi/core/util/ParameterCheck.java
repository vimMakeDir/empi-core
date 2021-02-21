package com.ksyun.empi.core.util;

import com.ksyun.empi.core.exception.ValidationException;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Date: 2021/02/07
 * Company: www.ksyun.com
 *
 * @author xuehan
 */
public class ParameterCheck {

    public static ParameterTool parameterCheck(String[] args){

        if (args.length == 0) {
            new ValidationException("参数列表为空,请输入正确参数列表.");
        }
        ParameterTool params=ParameterTool.fromArgs(args);

        return params;
    }
}
