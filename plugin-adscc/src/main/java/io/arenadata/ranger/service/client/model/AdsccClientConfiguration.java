package io.arenadata.ranger.service.client.model;

import javax.net.ssl.SSLContext;
import java.net.URI;

public class AdsccClientConfiguration {

    private final URI adsccUri;

    private final SSLContext sslContext;

    public AdsccClientConfiguration(URI adsccUri, SSLContext sslContext) {
        this.adsccUri = adsccUri;
        this.sslContext = sslContext;
    }

    public URI getAdsccUri() {
        return adsccUri;
    }

    public SSLContext getSslContext() {
        return sslContext;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AdsccClientConfiguration that = (AdsccClientConfiguration) o;

        if (adsccUri != null ? !adsccUri.equals(that.adsccUri) : that.adsccUri != null) return false;
        return sslContext != null ? sslContext.equals(that.sslContext) : that.sslContext == null;
    }

    @Override
    public int hashCode() {
        int result = adsccUri != null ? adsccUri.hashCode() : 0;
        result = 31 * result + (sslContext != null ? sslContext.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AdsccClientConfiguration{" +
                "adsccUri=" + adsccUri +
                ", sslContext=" + sslContext +
                '}';
    }
}
