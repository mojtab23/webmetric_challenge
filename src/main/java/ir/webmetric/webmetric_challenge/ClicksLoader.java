package ir.webmetric.webmetric_challenge;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@Component
public class ClicksLoader {

    private final Logger log = LoggerFactory.getLogger(ClicksLoader.class);
    private final ObjectMapper objectMapper;

    public ClicksLoader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public List<Click> load(Resource clicksFile) throws IOException {
        InputStream inputStream = clicksFile.getInputStream();

        List<Click> clicks = objectMapper.readValue(inputStream, new TypeReference<>() {});

        log.info("Loaded {} clicks", clicks.size());
        double sum = clicks.stream().mapToDouble(Click::revenue).sum();
        log.info("sumRevenue:{}", sum);

        return clicks;
    }


}
