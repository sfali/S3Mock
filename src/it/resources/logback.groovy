import ch.qos.logback.core.*
import ch.qos.logback.classic.encoder.PatternLayoutEncoder

appender(name="CONSOLE", clazz=ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "name=S3Mock date=%date{ISO8601} level=%level class=%logger{25} actor=%X{akkaSource} message=%msg\n"
    }
}

root(level=WARN, appenderNames=["CONSOLE"])
logger(name="software.amazon.awssdk.services.s3", level=DEBUG)
logger(name="com.loyalty", level=DEBUG)
logger(name="com.loyalty.testing.s3.actor", level=DEBUG)
logger(name="com.loyalty.testing.s3.repositories", level=DEBUG)
