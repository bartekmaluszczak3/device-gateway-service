package com.device.service.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

@Configuration
@Data
@ConfigurationProperties("service.well-known")
public class WellKnowConfiguration {
    private Resource caKeyResource;
    private String caKeyStorePassword;
    private String caKeyAlias;
    private int certValidityDays;

}
