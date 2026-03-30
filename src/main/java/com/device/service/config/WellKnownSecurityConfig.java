package com.device.service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@Order(1)
public class WellKnownSecurityConfig {
    @Bean
    public SecurityFilterChain estSecurityFilterChain(HttpSecurity http) throws Exception {
        http
                .securityMatcher("/.well-known/est/**")
                .x509(x509 -> x509
                        .subjectPrincipalRegex("CN=(.*?)(?:,|$)")
                        .userDetailsService(username ->
                                org.springframework.security.core.userdetails.User
                                        .withUsername(username)
                                        .password("")
                                        .roles("DEVICE")
                                        .build()
                        )
                )
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/.well-known/est/cacerts").permitAll()
                        .requestMatchers("/.well-known/est/simpleenroll").hasRole("DEVICE")
                        .requestMatchers("/.well-known/est/simplereenroll").hasRole("DEVICE")
                        .anyRequest().denyAll()
                )
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS));

        return http.build();
    }
}
