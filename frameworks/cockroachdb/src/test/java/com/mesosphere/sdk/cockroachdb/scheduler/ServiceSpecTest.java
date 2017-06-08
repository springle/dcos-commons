package com.mesosphere.sdk.cockroachdb.scheduler;

import com.mesosphere.sdk.testing.BaseServiceSpecTest;
import org.junit.BeforeClass;
import org.junit.Test;
import java.net.URL;
import java.io.File;

public class ServiceSpecTest extends BaseServiceSpecTest {

    @BeforeClass
    public static void beforeAll() {
        ENV_VARS.set("EXECUTOR_URI", "");
        ENV_VARS.set("LIBMESOS_URI", "");
        ENV_VARS.set("PORT_API", "8080");
        ENV_VARS.set("FRAMEWORK_NAME", "cockroachdb");

        ENV_VARS.set("NODE_COUNT", "2");
        ENV_VARS.set("NODE_CPUS", "0.1");
        ENV_VARS.set("NODE_MEM", "512");
        ENV_VARS.set("NODE_DISK", "5000");
        ENV_VARS.set("NODE_DISK_TYPE", "ROOT");

        ENV_VARS.set("SLEEP_DURATION", "1000");
        
        URL resource = ServiceSpecTest.class.getClassLoader().getResource("start.sh.mustache");
        ENV_VARS.set("CONFIG_TEMPLATE_PATH", new File(resource.getPath()).getParent());
    }

    @Test
    public void testYmlBase() throws Exception {
        testYaml("svc.yml");
    }
}
