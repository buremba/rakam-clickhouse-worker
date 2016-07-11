package io.rakam.clickhouse;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class RetryDriver
{
    private static final Logger log = Logger.get(RetryDriver.class);
    private static final int DEFAULT_RETRY_ATTEMPTS = 10;
    private static final Duration DEFAULT_SLEEP_TIME = Duration.valueOf("1s");
    private static final Duration DEFAULT_MAX_RETRY_TIME = Duration.valueOf("30s");
    private static final double DEFAULT_SCALE_FACTOR = 2.0;

    private final int maxAttempts;
    private final Duration minSleepTime;
    private final Duration maxSleepTime;
    private final double scaleFactor;
    private final Duration maxRetryTime;
    private final Function<Throwable, Throwable> exceptionMapper;
    private final List<Class<? extends Exception>> exceptionWhiteList;
    private final Optional<Runnable> retryRunnable;

    private RetryDriver(
            int maxAttempts,
            Duration minSleepTime,
            Duration maxSleepTime,
            double scaleFactor,
            Duration maxRetryTime,
            Function<Throwable, Throwable> exceptionMapper,
            List<Class<? extends Exception>> exceptionWhiteList,
            Optional<Runnable> retryRunnable)
    {
        this.maxAttempts = maxAttempts;
        this.minSleepTime = minSleepTime;
        this.maxSleepTime = maxSleepTime;
        this.scaleFactor = scaleFactor;
        this.maxRetryTime = maxRetryTime;
        this.exceptionMapper = exceptionMapper;
        this.exceptionWhiteList = exceptionWhiteList;
        this.retryRunnable = retryRunnable;
    }

    private RetryDriver()
    {
        this(DEFAULT_RETRY_ATTEMPTS,
                DEFAULT_SLEEP_TIME,
                DEFAULT_SLEEP_TIME,
                DEFAULT_SCALE_FACTOR,
                DEFAULT_MAX_RETRY_TIME,
                Function.identity(),
                ImmutableList.of(),
                Optional.empty());
    }

    public static RetryDriver retry()
    {
        return new RetryDriver();
    }

    public final RetryDriver maxAttempts(int maxAttempts)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, exceptionMapper, exceptionWhiteList, retryRunnable);
    }

    public final RetryDriver exponentialBackoff(Duration minSleepTime, Duration maxSleepTime, Duration maxRetryTime, double scaleFactor)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, exceptionMapper, exceptionWhiteList, retryRunnable);
    }

    public final RetryDriver onRetry(Runnable retryRunnable)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, exceptionMapper, exceptionWhiteList, Optional.ofNullable(retryRunnable));
    }

    public final RetryDriver exceptionMapper(Function<Throwable, Throwable> exceptionMapper)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, exceptionMapper, exceptionWhiteList, retryRunnable);
    }

    @SafeVarargs
    public final RetryDriver stopOn(Class<? extends Exception>... classes)
    {
        Objects.requireNonNull(classes, "classes is null");
        List<Class<? extends Exception>> exceptions = ImmutableList.<Class<? extends Exception>>builder()
                .addAll(exceptionWhiteList)
                .addAll(Arrays.asList(classes))
                .build();

        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, exceptionMapper, exceptions, retryRunnable);
    }

    public RetryDriver stopOnIllegalExceptions()
    {
        return stopOn(NullPointerException.class, IllegalStateException.class, IllegalArgumentException.class);
    }

    public <V> V run(String callableName, Callable<V> callable)
            throws Exception
    {
        Objects.requireNonNull(callableName, "callableName is null");
        Objects.requireNonNull(callable, "callable is null");

        long startTime = System.nanoTime();
        int attempt = 0;
        while (true) {
            attempt++;

            if (attempt > 1) {
                retryRunnable.ifPresent(Runnable::run);
            }

            try {
                return callable.call();
            }
            catch (Throwable e) {
                e = exceptionMapper.apply(e);
                for (Class<? extends Exception> clazz : exceptionWhiteList) {
                    if (clazz.isInstance(e)) {
                        throw new RuntimeException(e);
                    }
                }
                if (attempt >= maxAttempts || Duration.nanosSince(startTime).compareTo(maxRetryTime) >= 0) {
                    throw new RuntimeException(e);
                }
                log.debug("Failed on executing %s with attempt %d, will retry. Exception: %s", callableName, attempt, e.getMessage());

                int delayInMs = (int) Math.min(minSleepTime.toMillis() * Math.pow(scaleFactor, attempt - 1), maxSleepTime.toMillis());
                TimeUnit.MILLISECONDS.sleep(delayInMs);
            }
        }
    }
}

