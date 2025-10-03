/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.storage_backend.s3;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.time.Duration;
import java.util.Map;

import io.aiven.inkless.common.config.validators.NonEmptyPassword;
import io.aiven.inkless.common.config.validators.Null;
import io.aiven.inkless.common.config.validators.Subclass;
import io.aiven.inkless.common.config.validators.ValidUrl;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.utils.builder.Buildable;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class S3StorageConfig extends AbstractConfig {

    public static final String S3_BUCKET_NAME_CONFIG = "s3.bucket.name";
    private static final String S3_BUCKET_NAME_DOC = "S3 bucket to store log segments";
    public static final String S3_ENDPOINT_URL_CONFIG = "s3.endpoint.url";
    private static final String S3_ENDPOINT_URL_DOC = "Custom S3 endpoint URL. "
        + "To be used with custom S3-compatible backends (e.g. minio).";
    public static final String S3_REGION_CONFIG = "s3.region";
    private static final String S3_REGION_DOC = "AWS region where S3 bucket is placed";
    public static final String S3_PATH_STYLE_ENABLED_CONFIG = "s3.path.style.access.enabled";
    private static final String S3_PATH_STYLE_ENABLED_DOC = "Whether to use path style access or virtual hosts. "
        + "By default, empty value means S3 library will auto-detect. "
        + "Amazon S3 uses virtual hosts by default (true), but other S3-compatible backends may differ (e.g. minio).";

    private static final String S3_API_CALL_TIMEOUT_CONFIG = "s3.api.call.timeout";
    private static final String S3_API_CALL_TIMEOUT_DOC = "AWS S3 API call timeout in milliseconds, "
        + "including all retries";
    private static final String S3_API_CALL_ATTEMPT_TIMEOUT_CONFIG = "s3.api.call.attempt.timeout";
    private static final String S3_API_CALL_ATTEMPT_TIMEOUT_DOC = "AWS S3 API call attempt "
        + "(single retry) timeout in milliseconds";
    public static final String AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG = "aws.credentials.provider.class";
    private static final String AWS_CREDENTIALS_PROVIDER_CLASS_DOC = "AWS credentials provider. "
        + "If not set, AWS SDK uses the default "
        + "software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain";
    public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access.key.id";
    private static final String AWS_ACCESS_KEY_ID_DOC = "AWS access key ID. "
        + "To be used when static credentials are provided.";
    public static final String AWS_SECRET_ACCESS_KEY_CONFIG = "aws.secret.access.key";
    private static final String AWS_SECRET_ACCESS_KEY_DOC = "AWS secret access key. "
        + "To be used when static credentials are provided.";
    public static final String AWS_SESSION_TOKEN_CONFIG = "aws.session.token";
    private static final String AWS_SESSION_TOKEN_DOC = "The AWS session token. "
        + "Retrieved from an AWS token service, used for authenticating that this user has"
        + " received temporary permission to access some resource.";
    public static final String AWS_CREDENTIALS_FILE_CONFIG = "aws.credentials.file";
    private static final String AWS_CREDENTIALS_FILE_DOC =
        "This property is used to define a file where credentials are defined. "
            + "The file must contain AWS credentials in the format as those would be in the properties file ie. "
            + "inkless.storage.aws.access.key.id=, inkless.storage.aws.secret.access.key=, and inkless.storage.aws.session.token=."
            + "The file might be updated during process life cycle, "
            + "and the credentials will be reloaded from the file.";

    public static final String AWS_CERTIFICATE_CHECK_ENABLED_CONFIG = "aws.certificate.check.enabled";
    private static final String AWS_CERTIFICATE_CHECK_ENABLED_DOC =
        "This property is used to enable SSL certificate checking for AWS services. "
            + "When set to \"false\", the SSL certificate checking for AWS services will be bypassed. "
            + "Use with caution and always only in a test environment, as disabling certificate lead the storage "
            + "to be vulnerable to man-in-the-middle attacks.";

    public static final String AWS_HTTP_MAX_CONNECTIONS_CONFIG = "aws.http.max.connections";
    private static final String AWS_HTTP_MAX_CONNECTIONS_DOC =
        "This max number of HTTP connections to keep in the client pool.";

    public static final String AWS_CHECKSUM_CHECK_ENABLED_CONFIG = "aws.checksum.check.enabled";
    private static final String AWS_CHECKSUM_CHECK_ENABLED_DOC =
        "This property is used to enable checksum validation done by AWS library. "
            + "When set to \"false\", there will be no validation. "
            + "It is disabled by default as Kafka already validates integrity of the files.";


    public static ConfigDef configDef() {
        return new ConfigDef()
            .define(
                S3_BUCKET_NAME_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                S3_BUCKET_NAME_DOC)
            .define(
                S3_ENDPOINT_URL_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ValidUrl(),
                ConfigDef.Importance.LOW,
                S3_ENDPOINT_URL_DOC)
            .define(
                S3_REGION_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.MEDIUM,
                S3_REGION_DOC)
            .define(
                S3_PATH_STYLE_ENABLED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                null,
                ConfigDef.Importance.LOW,
                S3_PATH_STYLE_ENABLED_DOC)
            .define(
                S3_API_CALL_TIMEOUT_CONFIG,
                ConfigDef.Type.LONG,
                null,
                Null.or(ConfigDef.Range.between(1, Long.MAX_VALUE)),
                ConfigDef.Importance.LOW,
                S3_API_CALL_TIMEOUT_DOC)
            .define(
                S3_API_CALL_ATTEMPT_TIMEOUT_CONFIG,
                ConfigDef.Type.LONG,
                null,
                Null.or(ConfigDef.Range.between(1, Long.MAX_VALUE)),
                ConfigDef.Importance.LOW,
                S3_API_CALL_ATTEMPT_TIMEOUT_DOC)
            .define(
                AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                null,
                Subclass.of(AwsCredentialsProvider.class),
                ConfigDef.Importance.LOW,
                AWS_CREDENTIALS_PROVIDER_CLASS_DOC)
            .define(
                AWS_ACCESS_KEY_ID_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                new NonEmptyPassword(),
                ConfigDef.Importance.MEDIUM,
                AWS_ACCESS_KEY_ID_DOC)
            .define(
                AWS_SECRET_ACCESS_KEY_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                new NonEmptyPassword(),
                ConfigDef.Importance.MEDIUM,
                AWS_SECRET_ACCESS_KEY_DOC)
            .define(
                AWS_SESSION_TOKEN_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                new NonEmptyPassword(),
                ConfigDef.Importance.MEDIUM,
                AWS_SESSION_TOKEN_DOC)
            .define(
                AWS_CREDENTIALS_FILE_CONFIG,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.MEDIUM,
                AWS_CREDENTIALS_FILE_DOC
            )
            .define(AWS_CERTIFICATE_CHECK_ENABLED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.LOW,
                AWS_CERTIFICATE_CHECK_ENABLED_DOC
            )
            .define(AWS_HTTP_MAX_CONNECTIONS_CONFIG,
                ConfigDef.Type.INT,
                150,
                atLeast(50),
                ConfigDef.Importance.LOW,
                AWS_HTTP_MAX_CONNECTIONS_DOC
            )
            .define(AWS_CHECKSUM_CHECK_ENABLED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.MEDIUM,
                AWS_CHECKSUM_CHECK_ENABLED_DOC
            );
    }

    @SuppressWarnings({"this-escape"})
    public S3StorageConfig(final Map<String, ?> props) {
        super(configDef(), props);
        validate();
    }

    private void validate() {
        if (getPassword(AWS_ACCESS_KEY_ID_CONFIG) != null
            ^ getPassword(AWS_SECRET_ACCESS_KEY_CONFIG) != null) {
            throw new ConfigException(AWS_ACCESS_KEY_ID_CONFIG
                + " and "
                + AWS_SECRET_ACCESS_KEY_CONFIG
                + " must be defined together");
        }
        if (getClass(AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG) != null
            && getPassword(AWS_ACCESS_KEY_ID_CONFIG) != null) {
            throw new ConfigException("Either "
                + " static credential pair "
                + AWS_ACCESS_KEY_ID_CONFIG + " and " + AWS_SECRET_ACCESS_KEY_CONFIG
                + " must be set together, or a custom provider class "
                + AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG
                + ".");
        }
        if (getClass(AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG) != null
            && getString(AWS_CREDENTIALS_FILE_CONFIG) != null) {
            throw new ConfigException("Both '"
                + AWS_CREDENTIALS_FILE_CONFIG + "'' and '"
                + AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG
                + "' cannot be defined together");
        }
        if (getPassword(AWS_ACCESS_KEY_ID_CONFIG) != null
            && getString(AWS_CREDENTIALS_FILE_CONFIG) != null) {
            throw new ConfigException("Both '"
                + AWS_CREDENTIALS_FILE_CONFIG + "'' and static credential pair '"
                + AWS_ACCESS_KEY_ID_CONFIG + "' and '" + AWS_SECRET_ACCESS_KEY_CONFIG
                + "' cannot be defined together");
        }
    }

    Region region() {
        return Region.of(getString(S3_REGION_CONFIG));
    }

    Duration apiCallTimeout() {
        return getDuration(S3_API_CALL_TIMEOUT_CONFIG);
    }

    Duration apiCallAttemptTimeout() {
        return getDuration(S3_API_CALL_ATTEMPT_TIMEOUT_CONFIG);
    }

    AwsCredentialsProvider credentialsProvider() {
        @SuppressWarnings("unchecked") final Class<? extends AwsCredentialsProvider> providerClass =
            (Class<? extends AwsCredentialsProvider>) getClass(AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG);
        final boolean credentialsProvided =
            getPassword(AWS_ACCESS_KEY_ID_CONFIG) != null
                && getPassword(AWS_SECRET_ACCESS_KEY_CONFIG) != null;
        if (credentialsProvided) {
            if (getPassword(AWS_SESSION_TOKEN_CONFIG) != null) {
                final AwsCredentials credentials = AwsSessionCredentials.create(
                    getPassword(AWS_ACCESS_KEY_ID_CONFIG).value(),
                    getPassword(AWS_SECRET_ACCESS_KEY_CONFIG).value(),
                    getPassword(AWS_SESSION_TOKEN_CONFIG).value()
                );
                return StaticCredentialsProvider.create(credentials);
            }
            final AwsCredentials staticCredentials = AwsBasicCredentials.create(
                getPassword(AWS_ACCESS_KEY_ID_CONFIG).value(),
                getPassword(AWS_SECRET_ACCESS_KEY_CONFIG).value()
            );
            return StaticCredentialsProvider.create(staticCredentials);
        } else if (getString(AWS_CREDENTIALS_FILE_CONFIG) != null) {
            return new S3RotatingCredentialsProvider(getString(AWS_CREDENTIALS_FILE_CONFIG));
        } else if (providerClass == null) {
            return null; // to use S3 default provider chain. no public constructor
        } else if (StaticCredentialsProvider.class.isAssignableFrom(providerClass)) {
            throw new ConfigException("With " + StaticCredentialsProvider.class.getName()
                + " AWS credentials must be provided");
        } else {
            final Class<?> klass = getClass(AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG);
            try {
                final var builder = klass.getMethod("builder");
                return (AwsCredentialsProvider) ((Buildable) builder.invoke(klass)).build();
            } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                try {
                    final Method create = klass.getMethod("create");
                    return (AwsCredentialsProvider) create.invoke(klass);
                } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
                    throw new RuntimeException("Specified AWS credentials provider is unsupported", ex);
                }
            }
        }
    }

    public Boolean certificateCheckEnabled() {
        return getBoolean(AWS_CERTIFICATE_CHECK_ENABLED_CONFIG);
    }

    public int httpMaxConnections() {
        return getInt(AWS_HTTP_MAX_CONNECTIONS_CONFIG);
    }

    public Boolean checksumCheckEnabled() {
        return getBoolean(AWS_CHECKSUM_CHECK_ENABLED_CONFIG);
    }

    public String bucketName() {
        return getString(S3_BUCKET_NAME_CONFIG);
    }

    public Boolean pathStyleAccessEnabled() {
        return getBoolean(S3_PATH_STYLE_ENABLED_CONFIG);
    }

    URI s3ServiceEndpoint() {
        final String url = getString(S3_ENDPOINT_URL_CONFIG);
        if (url != null) {
            return URI.create(url);
        } else {
            return null;
        }
    }

    private Duration getDuration(final String key) {
        final var value = getLong(key);
        if (value != null) {
            return Duration.ofMillis(value);
        } else {
            return null;
        }
    }
}
