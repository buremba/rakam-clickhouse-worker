package io.rakam.clickhouse;

import com.getsentry.raven.RavenFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Inject;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.rakam.clickhouse.data.ClickhouseClusterShardManager;
import io.rakam.clickhouse.data.KinesisWorkerManager;
import io.rakam.clickhouse.data.backup.BackupService;
import io.rakam.clickhouse.data.backup.RecoveryManager;
import org.rakam.aws.AWSConfig;
import org.rakam.aws.dynamodb.metastore.DynamodbMetastoreConfig;
import org.rakam.clickhouse.ClickHouseConfig;
import org.rakam.server.http.HttpServerBuilder;
import org.rakam.server.http.HttpService;

import javax.annotation.PostConstruct;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class ServiceStarter
{
    private static final Logger logger = Logger.get(MetastoreWorkerManager.class);

    static {
        RavenFactory.ravenInstance();
    }

    public static void main(String[] args)
            throws Exception
    {
        Bootstrap bootstrap = new Bootstrap(new AbstractConfigurationAwareModule()
        {
            @Override
            protected void setup(Binder binder)
            {
                configBinder(binder).bindConfig(BackupConfig.class);
                configBinder(binder).bindConfig(AWSConfig.class);
                configBinder(binder).bindConfig(ClickHouseConfig.class);
                configBinder(binder).bindConfig(StreamConfig.class);
                configBinder(binder).bindConfig(DynamodbMetastoreConfig.class);
                binder.bind(HttpService.class).to(ClickhouseClusterShardManager.class);
                binder.bind(ShardHttpServer.class).asEagerSingleton();
                binder.bind(BackupService.class).asEagerSingleton();
                binder.bind(RecoveryManager.class).asEagerSingleton();
                binder.bind(MetastoreWorkerManager.class).asEagerSingleton();
                binder.bind(KinesisWorkerManager.class).asEagerSingleton();
            }
        });

        logger.info("------ SERVICE INITIALIZING ------");
        logger.error("------ SERVICE INITIALIZING ------");
        bootstrap.strictConfig().initialize();
        logger.error("------ SERVICE STARTED ------");
    }

    private static class ShardHttpServer
    {

        private final HttpService service;

        @Inject
        public ShardHttpServer(HttpService service)
        {
            this.service = service;
        }

        @PostConstruct
        protected void setup()
        {
            try {
                new HttpServerBuilder().setHttpServices(
                        ImmutableSet.of(service)).build()
                        .bind("0.0.0.0", 5466);
            }
            catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
