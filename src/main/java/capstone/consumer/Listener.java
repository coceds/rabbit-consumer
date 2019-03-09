package capstone.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.logging.Logger;

@SpringBootApplication
@EnableRabbit
public class Listener {

    private static Logger logger = Logger.getLogger("Listener");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final long timestamp = System.currentTimeMillis();

    public static void main(String[] args) {
        SpringApplication.run(Listener.class, args);
    }

    @RabbitListener(queues = "q.example")
    public void onMessage(byte[] message) throws Exception {
        Order order = MAPPER.readValue(message, Order.class);
        logger.info((System.currentTimeMillis() - timestamp) + " : " + order.toString());
    }

    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        connectionFactory.setAddresses("192.168.99.100:30000,192.168.99.100:30002,192.168.99.100:30004");
        connectionFactory.setChannelCacheSize(10);
        return connectionFactory;
    }

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory());
        factory.setConcurrentConsumers(1);
        factory.setMaxConcurrentConsumers(1);
        return factory;
    }

    @Bean
    public Queue queue() {
        return new Queue("q.example");
    }

}
