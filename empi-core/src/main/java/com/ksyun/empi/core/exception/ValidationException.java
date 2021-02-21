package com.ksyun.empi.core.exception;


/**
 * Date: 2021/02/07
 * Company: www.ksyun.com
 *
 * @author xuehan
 */
public class ValidationException extends RuntimeException {

    private static final long serialVersionUID = -5235890872962251059L;

    public ValidationException() {
        super();
    }

    public ValidationException(String message) {
        super(message);
        System.out.println(message);
    }

    public ValidationException(Throwable exception) {
        super(exception);
    }

    public ValidationException(String message, Throwable exception) {
        super(message, exception);
    }

}
