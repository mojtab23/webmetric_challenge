package ir.webmetric.webmetric_challenge;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Component
public class AppRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(AppRunner.class);
    private final Resource clicksFile;
    private final Resource impressionsFile;
    private final ClicksLoader clicksLoader;
    private final ImpressionsLoader impressionsLoader;
    private final ObjectMapper objectMapper;
    private final boolean runBenchmark;

    public AppRunner(
            @Value("${myapp.run-benchmark}") Boolean runBenchmark,
            @Value("classpath:input/clicks.json") Resource clicksFile,
            @Value("classpath:input/impressions.json") Resource impressionsFile,
            ClicksLoader clicksLoader,
            ImpressionsLoader impressionsLoader,
            ObjectMapper objectMapper) {

        this.runBenchmark = runBenchmark;
        this.clicksFile = clicksFile;
        this.impressionsFile = impressionsFile;
        this.clicksLoader = clicksLoader;
        this.impressionsLoader = impressionsLoader;
        this.objectMapper = objectMapper;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<Click> clicks = clicksLoader.load(clicksFile);
        List<Impression> impressions = impressionsLoader.load(impressionsFile);
        Collection<PointTwoAggregation> aggs1 = clickToImpression(clicks, impressions);
        if (runBenchmark) {
            Collection<PointTwoAggregation> aggs2 = clickToImpressionWithStreams(clicks, impressions);
            String benchmarkFor = benchmarkFor(clicks, impressions);
            String benchmarkStream = benchmarkStream(clicks, impressions);

            log.info("##################### Benchmark result #####################");
            log.info(benchmarkFor);
            log.info(benchmarkStream);

            if (aggs1.size() == aggs2.size() && aggs1.containsAll(aggs2) && aggs2.containsAll(aggs1)) {
                log.info("implementations are the same");
            } else {
                log.error("implementations are not the same");
            }
        }
        serialize(aggs1);

    }

    private String benchmarkFor(List<Click> clicks, List<Impression> impressions) {

        double avgTime = 0.0;
        int size = 0;
        for (int i = 0; i < 100; i++) {
            long startTime = System.currentTimeMillis();
            Collection<PointTwoAggregation> aggs = clickToImpression(clicks, impressions);
            // reading the aggs to prevent possible runtime optimisations
            size = aggs.size();

            long endTime = System.currentTimeMillis();
            long execTime = endTime - startTime;
            avgTime += (double) execTime / 100;
        }

        return String.format("Avg time with SingleThread For loop in 100 rounds: %,f  ms , size:%d", avgTime, size);
    }

    private String benchmarkStream(List<Click> clicks, List<Impression> impressions) {

        double avgTime = 0.0;
        int size = 0;
        for (int i = 0; i < 100; i++) {
            long startTime = System.currentTimeMillis();
            Collection<PointTwoAggregation> aggs = clickToImpressionWithStreams(clicks, impressions);
            // reading the aggs to prevent possible runtime optimisations
            size = aggs.size();
            long endTime = System.currentTimeMillis();
            long execTime = endTime - startTime;
            avgTime += (double) execTime / 100;
        }
        return String.format("Avg time with Parallel Stream in 100 rounds: %,f ms , size:%d", avgTime, size);
    }

    private Collection<PointTwoAggregation> clickToImpression(List<Click> clicks, List<Impression> impressions) {
        Map<String, List<Click>> clickMap = clicks.parallelStream().collect(Collectors.groupingBy(Click::impressionId));

        int impressionsWithNoClickCount = 0;

        ConcurrentHashMap<String, PointTwoAggregation> aggMap = new ConcurrentHashMap<>();


        for (Impression impression : impressions) {
            List<Click> imprClicks = clickMap.get(impression.id());
            if (imprClicks == null || imprClicks.isEmpty()) {
                impressionsWithNoClickCount++;
                continue;
            }
            final Integer appId = impression.appId();
            if (appId == null) {
                log.warn("impression:{} appId is null", impression.id());
                continue;
            }
            final String countryCode;
            if (impression.countryCode() == null || impression.countryCode().isBlank()) {
                countryCode = "";
            } else {
                countryCode = impression.countryCode();
            }
            if (countryCode.length() > 2) {
                log.warn("invalid countryCode: {}", countryCode);
                continue;
            }
            var key = String.format("%d.%s", appId, countryCode);

            Double imprRevenue = imprClicks.stream().mapToDouble(Click::revenue).sum();

            aggMap.compute(key, (_k, previousValue) -> {
                if (previousValue == null) {
                    return new PointTwoAggregation(
                            appId,
                            countryCode,
                            1,
                            imprClicks.size(),
                            imprRevenue);
                }
                return new PointTwoAggregation(
                        appId,
                        countryCode,
                        previousValue.impressions() + 1,
                        previousValue.clicks() + imprClicks.size(),
                        previousValue.revenue() + imprRevenue);
            });
        }
        Collection<PointTwoAggregation> aggs = aggMap.values();

        printInfo(impressions, aggMap, impressionsWithNoClickCount, aggs);
        return aggs;
    }

    private Collection<PointTwoAggregation> clickToImpressionWithStreams(List<Click> clicks, List<Impression> impressions) {
        Map<String, List<Click>> clickMap = clicks.parallelStream().collect(Collectors.groupingBy(Click::impressionId));

        final AtomicInteger impressionsWithNoClickCount = new AtomicInteger(0);

        ConcurrentHashMap<String, PointTwoAggregation> aggMap = new ConcurrentHashMap<>();


        impressions.parallelStream().forEach(impression -> {
            List<Click> imprClicks = clickMap.get(impression.id());
            if (imprClicks == null || imprClicks.isEmpty()) {
                impressionsWithNoClickCount.incrementAndGet();
                return;
            }
            final Integer appId = impression.appId();
            if (appId == null) {
                log.warn("impression:{} appId is null", impression.id());
                return;
            }
            final String countryCode;
            if (impression.countryCode() == null || impression.countryCode().isBlank()) {
                countryCode = "";
            } else {
                countryCode = impression.countryCode();
            }
            if (countryCode.length() > 2) {
                log.warn("invalid countryCode: {}", countryCode);
                return;
            }
            var key = String.format("%d.%s", appId, countryCode);

            Double imprRevenue = imprClicks.stream().mapToDouble(Click::revenue).sum();

            aggMap.compute(key, (_k, previousValue) -> {
                if (previousValue == null) {
                    return new PointTwoAggregation(
                            appId,
                            countryCode,
                            1,
                            imprClicks.size(),
                            imprRevenue);
                }
                return new PointTwoAggregation(
                        appId,
                        countryCode,
                        previousValue.impressions() + 1,
                        previousValue.clicks() + imprClicks.size(),
                        previousValue.revenue() + imprRevenue);
            });

        });

        Collection<PointTwoAggregation> aggs = aggMap.values();

        printInfo(impressions, aggMap, impressionsWithNoClickCount.get(), aggs);
        return aggs;
    }

    private static void printInfo(List<Impression> impressions, ConcurrentHashMap<String, PointTwoAggregation> aggMap, int impressionsWithNoClickCount, Collection<PointTwoAggregation> aggs) {
        log.info("Agg:{}", aggMap);
        log.info("impressionsWithNoClickCount:{}", impressionsWithNoClickCount);
        int impWithClickCount = aggs.stream().mapToInt(PointTwoAggregation::impressions).sum();
        log.info("impWithClickCount:{}", impWithClickCount);
        if (impWithClickCount + impressionsWithNoClickCount != impressions.size()) {
            log.error("compute error: imps count");
        }

        double sumRevenue = aggs.stream().mapToDouble(PointTwoAggregation::revenue).sum();
        log.info("sumRevenue:{}", sumRevenue);

        // sum from jq '[.[].revenue] | add' clicks.json
        if (sumRevenue != 142.13500956606202) {
            log.error("compute error: sum revenue");
        }
    }

    private void serialize(Collection<PointTwoAggregation> aggs) {
        try {
            Path path = Path.of("./output.json");
            FileWriter fileWriter = new FileWriter(path.toFile(), false);
            objectMapper.writeValue(fileWriter, aggs);
            log.info("output json file: {}", path.toAbsolutePath());
        } catch (IOException e) {
            log.error("error serializing the result", e);
        }
    }

}
