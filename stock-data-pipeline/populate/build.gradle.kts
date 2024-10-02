plugins {
    kotlin("jvm") version "1.8.0" // Adjust to match your Kotlin version
    kotlin("plugin.serialization") version "1.8.0" // Serialization plugin for Kotlin
    id("com.github.johnrengelman.shadow") version "7.1.2" // ShadowJar plugin
    application
}

group = "org.populate"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    // Add JitPack repository for external dependencies
    maven { url = uri("https://jitpack.io") }
}

dependencies {
    // Kotlin Standard Library
    implementation(kotlin("stdlib"))

    // Kotlin Coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.4")

    // Apache Spark 3.5.2
    implementation("org.apache.spark:spark-core_2.12:3.5.2")
    implementation("org.apache.spark:spark-sql_2.12:3.5.2")

    // Polygon.io Kotlin SDK
    implementation("com.github.polygon-io:client-jvm:5.1.2")

    // Dotenv for managing environment variables
    implementation("io.github.cdimascio:dotenv-kotlin:6.2.2")

    // OkHttp for HTTP requests
    implementation("com.squareup.okhttp3:okhttp:4.9.3")

    // Kotlinx Serialization for JSON
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.5.0")



    // Testing dependencies
    testImplementation(kotlin("test")) // Kotlin test framework
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.2")
}

application {
    mainClass.set("org.populate.populateKt") // Set your actual main class
}

tasks.withType<Test> {
    useJUnitPlatform() // Enables JUnit 5 for testing
    enabled = false
}

// ShadowJar Configuration for fat JAR
tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    archiveBaseName.set("${project.name}-all")
    archiveClassifier.set("") // No classifier, default is empty
    mergeServiceFiles() // Merge service files required by Spark, etc.
    manifest {
        attributes["Main-Class"] = "org.populate.populateKt" // Adjust this to the main class
    }
    isZip64 = true
}

// Ensure compatibility with DSL 5.1.1 (General Gradle Syntax Adherence)
tasks {
    build {
        dependsOn("shadowJar")
    }
    jar {
        enabled = false // Disable the regular jar task if ShadowJar is used
    }
}
