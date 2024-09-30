package ir.webmetric.webmetric_challenge;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.List;

@Component
public class ImpressionsLoader {

    private final Logger log = LoggerFactory.getLogger(ImpressionsLoader.class);
    private final ObjectMapper objectMapper;

    public ImpressionsLoader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public List<Impression> load(Resource impressionsFile) throws IOException {
        File file = impressionsFile.getFile();

        List<Impression> impressions = objectMapper.readValue(file, new TypeReference<>() {});
        impressions = impressions.stream().distinct().toList();

        log.info("Loaded {} impressions", impressions.size());
        return impressions;
    }

}
