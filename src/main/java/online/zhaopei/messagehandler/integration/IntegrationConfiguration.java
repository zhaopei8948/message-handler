package online.zhaopei.messagehandler.integration;

import com.ibm.mq.jms.MQQueue;
import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import online.zhaopei.messagehandler.configuration.MessageHandlerProp;
import online.zhaopei.messagehandler.constant.ChannelConstant;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.file.transformer.FileToByteArrayTransformer;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.messaging.MessageChannel;

import javax.annotation.PostConstruct;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

@Configuration
@EnableIntegration
public class IntegrationConfiguration {

    private static final Log logger = LogFactory.getLog(IntegrationConfiguration.class);

    private static BlockingQueue<Integer> CACHE_QUEUE;

    @Autowired
    private MessageHandlerProp messageHandlerProp;

    @Autowired
    private MQQueue mqQueue;

    @Autowired
    private JmsTemplate jmsTemplate;

    @Autowired
    private ExecutorService executorService;

    @PostConstruct
    public void initProp() {
        CACHE_QUEUE = new LinkedBlockingQueue<Integer>(this.messageHandlerProp.getCacheSize());
    }

    @Bean(ChannelConstant.CHANNEL_RECEIVE)
    public MessageChannel receiveChannel() {
        return new DirectChannel();
    }

    @Bean(ChannelConstant.CHANNEL_BYTE_RECEIVE)
    public MessageChannel receiveByteChannel() {
        return new DirectChannel();
    }

    @Bean
    public ExecutorService taskExecutor() {
        return Executors.newFixedThreadPool(this.messageHandlerProp.getPoolSize());
    }

    @Bean
    @InboundChannelAdapter(value = ChannelConstant.CHANNEL_RECEIVE, poller = @Poller(fixedDelay = "${message-handler.scanRate}", maxMessagesPerPoll = "${message-handler.messageCountOnce}"))
    public MessageSource<File> fileReadingMessageSource() {
        FileReadingMessageSource source = new FileReadingMessageSource();
        source.setDirectory(new File(this.messageHandlerProp.getReceiveDir()));
        source.setFilter(new SimplePatternFileListFilter(this.messageHandlerProp.getReceiveFilePattern()));
        return source;
    }

    @Bean
    @Transformer(inputChannel = ChannelConstant.CHANNEL_RECEIVE, outputChannel = ChannelConstant.CHANNEL_BYTE_RECEIVE)
    public FileToByteArrayTransformer fileToByteArrayTransformer() {
        FileToByteArrayTransformer fileToByteArrayTransformer = new FileToByteArrayTransformer();
        fileToByteArrayTransformer.setDeleteFiles(true);
        return fileToByteArrayTransformer;
    }

    @Bean
    public ConnectionFactory connectionFactory() throws Exception {
        CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
        cachingConnectionFactory.setSessionCacheSize(this.messageHandlerProp.getSessionCacheSize());
        MQQueueConnectionFactory mqQueueConnectionFactory = new MQQueueConnectionFactory();
        mqQueueConnectionFactory.setHostName(this.messageHandlerProp.getHostName());
        mqQueueConnectionFactory.setPort(this.messageHandlerProp.getPort());
        mqQueueConnectionFactory.setQueueManager(this.messageHandlerProp.getQueueManager());
        mqQueueConnectionFactory.setChannel(this.messageHandlerProp.getChannel());
        mqQueueConnectionFactory.setCCSID(this.messageHandlerProp.getCcsid());
        mqQueueConnectionFactory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
        cachingConnectionFactory.setTargetConnectionFactory(mqQueueConnectionFactory);
        return cachingConnectionFactory;
    }

    @Bean
    public MQQueue mqQueue() {
        MQQueue queue = new MQQueue();
        try {
            queue.setTargetClient(WMQConstants.WMQ_CLIENT_NONJMS_MQ);
            queue.setCCSID(this.messageHandlerProp.getCcsid());
            queue.setBaseQueueName(this.messageHandlerProp.getQueueName());
        } catch (JMSException e) {
            logger.error("init mqqueue error", e);
        }
        return queue;
    }

    @ServiceActivator(inputChannel = ChannelConstant.CHANNEL_BYTE_RECEIVE)
    public void sendByteMessage(byte[] bytes) {
        try {
            CACHE_QUEUE.put(1);
        } catch (InterruptedException e) {
            logger.error("cache put error", e);
        }
        this.executorService.execute(() -> {
            long startTime = System.nanoTime();
            CACHE_QUEUE.poll();
            this.jmsTemplate.convertAndSend(this.mqQueue, bytes);
            logger.info("cache size [" + CACHE_QUEUE.size() + "] send message use["
                    + ((double)(System.nanoTime() - startTime) / 1000000.0) + "]ms");
        });
    }
}
