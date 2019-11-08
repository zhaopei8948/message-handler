package online.zhaopei.messagehandler.utils;

import org.apache.commons.logging.Log;

import java.io.PrintWriter;
import java.io.StringWriter;

public class CommonUtils {

    public static void logError(Log log, Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        log.error(sw.toString());
    }

    public static int getRandomIndex(int length) {
        return (int) (Math.random() * length);
    }

    //public static void main(String[] args) {
    //    for (int i = 0; i < 10; i++) {
    //        System.out.println(getRandomIndex(1));
    //    }
    //}
}
