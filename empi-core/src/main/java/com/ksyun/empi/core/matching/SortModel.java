package com.ksyun.empi.core.matching;

import java.io.IOException;


/**
 * Date: 2021/02/07
 * Company: www.ksyun.com
 *
 * @author xuehan
 */
public abstract class SortModel {

    public abstract void getResult(MatchingInstance matchingInstance) throws IOException;
}
