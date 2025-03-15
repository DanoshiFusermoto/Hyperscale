plugins {
    `java-library`
    id("com.diffplug.spotless") version "7.0.2"
}

repositories {
    mavenCentral()
}

configurations.all {
    exclude(group = "unused-group", module = "unused-module")
}

tasks.jar {
    archiveFileName.set("hyperscale.jar")
    manifest {
        attributes["Main-Class"] = "org.radix.hyperscale.Hyperscale"
    }
    
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    
    from({
        configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }.map { zipTree(it) }
    }) {
        // Exclude all signature files
        exclude("META-INF/*.SF")
        exclude("META-INF/*.DSA")
        exclude("META-INF/*.RSA")
        exclude("META-INF/MANIFEST.MF")
        exclude("META-INF/LICENSE")
        exclude("META-INF/NOTICE")
    }
}

spotless {
    java {
        importOrder()
        removeUnusedImports()
        cleanthat()
        googleJavaFormat()
        formatAnnotations()  // fixes formatting of type annotations, see below
    }
}

dependencies {
    // JUnit 4 dependencies
    testImplementation("junit:junit:4.13.2")

    // This dependency is exported to consumers, that is to say found on their compile classpath.
    api(libs.commons.math3)

    // This dependency is used internally, and not exposed to consumers on their own compile classpath.
    implementation(libs.guava)
    
    implementation("commons-cli:commons-cli:1.4")
    implementation("org.apache.commons:commons-lang3:3.17.0")
    implementation("org.apache.commons:commons-compress:1.20")
    implementation("commons-io:commons-io:2.8.0")
    implementation("com.google.guava:guava:29.0-jre")
    implementation("org.java-websocket:Java-WebSocket:1.5.1")
    implementation("javax.servlet:javax.servlet-api:4.0.1")
    implementation("com.sleepycat:je:18.3.12")
    implementation("org.eclipse.jetty:jetty-server:9.4.27.v20200227")
    implementation("org.eclipse.jetty:jetty-webapp:9.4.27.v20200227")
    implementation("org.eclipse.jetty.websocket:websocket-server:9.4.25.v20191220")
    implementation("org.json:json:20201115")
    implementation("io.netty:netty-all:4.1.70.Final")
    implementation("org.reflections:reflections:0.9.12")
    implementation("org.slf4j:slf4j-api:1.7.30")
    implementation("org.slf4j:slf4j-simple:1.7.30")
    implementation("org.xerial.snappy:snappy-java:1.1.8.2")
    implementation("com.sparkjava:spark-core:2.9.3")
    implementation("org.javassist:javassist:3.29.0-GA")
    implementation("com.google.guava:failureaccess:1.0.1")

    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:2.13.5")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-json-org:2.13.5")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-guava:2.13.5")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:2.13.5")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.13.5")
    implementation("com.fasterxml.jackson.core:jackson-core:2.13.5")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.5")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-eclipse-collections:2.13.5")
    
    implementation("org.eclipse.collections:eclipse-collections:11.0.0")
    implementation("org.eclipse.collections:eclipse-collections-api:11.0.0")
    
    implementation("org.graalvm.sdk:graal-sdk:23.1.2")
    implementation("org.graalvm.js:js-language:23.1.2")
    implementation("org.graalvm.regex:regex:23.1.2")
    implementation("org.graalvm.truffle:truffle-api:23.1.2")
    implementation("org.graalvm.polyglot:polyglot:23.1.2")
    implementation("org.graalvm.sdk:collections:23.1.2")
    implementation("org.graalvm.sdk:word:23.1.2")
    implementation("org.graalvm.sdk:nativeimage:23.1.2")
    
    // Testing dependencies
    testImplementation("org.mockito:mockito-core:5.3.1")
    testImplementation("org.assertj:assertj-core:3.25.0")
    testImplementation("nl.jqno.equalsverifier:equalsverifier:3.16.2")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

// Standard source sets configuration
sourceSets {
    main {
        java {
            srcDirs("src/main/java")
        }
    }
    
    test {
        java {
            srcDirs("src/test/java")
        }
    }
}

// Configure the test task to run JUnit 4 tests
tasks.test {
    useJUnit()  // This configures Gradle to use JUnit 4
    testLogging {
        events("passed", "skipped", "failed")
    }
}