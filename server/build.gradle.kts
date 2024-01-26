import com.google.protobuf.gradle.*

plugins {
    id("java")
    id("com.google.protobuf") version "0.9.4"
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "org.example"

var mainClass = "raft.RaftNodeMain"

tasks.shadowJar {
    archiveBaseName.set("server")
    archiveClassifier.set("")
    archiveVersion.set("")

    mergeServiceFiles()

    manifest {
        attributes("Main-Class" to mainClass)
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

}

repositories {
    mavenCentral()
}

protobuf {
    protoc {
        // The artifact spec for the Protobuf Compiler
        artifact = "com.google.protobuf:protoc:3.6.1"
    }
    plugins {
        // Optional: an artifact spec for a protoc plugin, with "grpc" as
        // the identifier, which can be referred to in the "plugins"
        // container of the "generateProtoTasks" closure.
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.61.0"
        }
        id("reactor") {
            artifact = "com.salesforce.servicelibs:reactor-grpc:1.2.4"
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                // Apply the "grpc" plugin whose spec is defined above, without
                // options. Note the braces cannot be omitted, otherwise the
                // plugin will not be added. This is because of the implicit way
                // NamedDomainObjectContainer binds the methods.
                id("grpc") { }
                id("reactor") { }
            }
        }
    }
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.25.1")
    testImplementation("org.mockito:mockito-core:5.8.0")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.10.0")

    // gRPC
    implementation("com.google.protobuf:protobuf-java:3.22.2")
    implementation("io.grpc:grpc-stub:1.53.0")
    implementation("io.grpc:grpc-protobuf:1.53.0")
    implementation("io.grpc:grpc-netty:1.60.1")
    implementation("io.grpc:grpc-netty-shaded:1.60.1")

    implementation("com.salesforce.servicelibs:reactor-grpc-stub:1.2.4")

    implementation("io.projectreactor:reactor-core:3.6.2")

    // FlatBuffers
    implementation("com.google.flatbuffers:flatbuffers-java:23.5.26")

    if (JavaVersion.current().isJava9Compatible) {
        implementation("javax.annotation:javax.annotation-api:1.3.1")
    }
    // Logging
    implementation("org.slf4j:slf4j-api:2.1.0-alpha0")
    implementation("ch.qos.logback:logback-classic:1.4.14")

    // Dependency injection
    implementation("com.google.dagger:dagger:2.50")
    annotationProcessor("com.google.dagger:dagger-compiler:2.50")
}

tasks.test {
    useJUnitPlatform()
}