package com.weavelab.interview.ratelimit.service;

/** Shared lifecycle; admission contracts separately express synchronous or queued behavior. */
public interface IRateLimiter extends AutoCloseable {
    /** Permanently stops admission and settles pending work; repeated calls are harmless. */
    @Override
    void close();
}
