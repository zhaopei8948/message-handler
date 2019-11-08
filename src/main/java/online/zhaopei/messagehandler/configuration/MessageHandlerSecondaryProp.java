package online.zhaopei.messagehandler.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Setter
@Getter
@ConfigurationProperties(prefix = "message-handler.secondary")
@Component
public class MessageHandlerSecondaryProp {

    private String hostName;

    private Integer port;

    private String queueManager;

    private String channel;

    private Integer ccsid;

    private String queueName;

    private Integer minConcurrency;

    private Integer maxConcurrency;

    private Integer sessionCacheSize;
}
