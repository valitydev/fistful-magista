package dev.vality.fistful.magista.config;

import dev.vality.fistful.identity.ManagementSrv;
import dev.vality.fistful.magista.config.properties.IdentityManagementProperties;
import dev.vality.woody.thrift.impl.http.THSpawnClientBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class ClientConfig {

    @Bean
    public ManagementSrv.Iface identityManagementClient(IdentityManagementProperties properties) throws IOException {
        return new THSpawnClientBuilder()
                .withAddress(properties.getUrl().getURI())
                .withNetworkTimeout(properties.getTimeout())
                .build(ManagementSrv.Iface.class);
    }
}
