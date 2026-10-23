package com.weavelab.interview.config;

import com.weavelab.interview.auth.AuthFilter;
import com.weavelab.interview.ratelimit.ImmediateRateLimitFilter;
import com.weavelab.interview.ratelimit.QueuedRateLimitFilter;
import com.weavelab.interview.ratelimit.RateLimitFilter;
import com.weavelab.interview.ratelimit.service.IImmediateRateLimiter;
import com.weavelab.interview.ratelimit.service.IQueuedRateLimiter;
import com.weavelab.interview.ratelimit.service.IRateLimiter;

import jakarta.servlet.DispatcherType;
import jakarta.servlet.Filter;

import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * Registers exactly one rate-limit HTTP adapter and the authentication filter, in that order.
 * Selects the adapter from the admission contract and enables servlet async redispatch.
 */
@Configuration(proxyBeanMethods = false)
public class FilterConfig {
    @Bean
    FilterRegistrationBean<RateLimitFilter> rateLimitFilterRegistration(
            IRateLimiter limiter, RateLimitProperties properties) {
        FilterRegistrationBean<RateLimitFilter> registration =
                new FilterRegistrationBean<>(rateLimitFilter(limiter, properties));
        register(registration, 0);
        return registration;
    }

    @Bean
    FilterRegistrationBean<AuthFilter> authFilterRegistration(AuthFilter authFilter) {
        FilterRegistrationBean<AuthFilter> registration = new FilterRegistrationBean<>(authFilter);
        register(registration, 1);
        return registration;
    }

    private RateLimitFilter rateLimitFilter(IRateLimiter limiter, RateLimitProperties properties) {
        if (limiter instanceof IImmediateRateLimiter immediate) {
            return new ImmediateRateLimitFilter(immediate, properties, Clock.systemUTC());
        } else if (limiter instanceof IQueuedRateLimiter queued) {
            return new QueuedRateLimitFilter(queued, properties, Clock.systemUTC());
        }

        throw new IllegalArgumentException("Unsupported rate limiter admission contract");
    }

    private <T extends Filter> void register(FilterRegistrationBean<T> registration, int order) {
        registration.setOrder(order);
        registration.setAsyncSupported(true);
        registration.setDispatcherTypes(DispatcherType.REQUEST, DispatcherType.ASYNC);
        registration.addUrlPatterns("/*");
    }
}
