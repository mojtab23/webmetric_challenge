package ir.webmetric.webmetric_challenge;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Click(String impressionId, Double revenue) {
}
