package integrationtest.com.weavelab.interview;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(classes = com.weavelab.interview.Application.class)
@TestPropertySource(
        properties = {
            "app.data-dir=target/test-data",
            "spring.datasource.url=jdbc:sqlite:target/test-data/app.db",
            "app.startup-check=false"
        })
class ApplicationTests {

    @Test
    void contextLoads() {}
}
