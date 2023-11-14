import org.gradle.internal.impldep.org.apache.maven.model.Build

plugins {
    id("java")
    application
    checkstyle
    pmd
}

group = "com.github.aayushjn"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.google.code.gson:gson:2.10.1")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

application {
    mainClass = "com.github.aayushjn.keyvaluestore.KeyValueStore"
}

tasks {
    withType<Test> {
        useJUnitPlatform()
    }

    withType<JavaExec> {
        standardInput = System.`in`
    }

    withType<Jar> {
        from(sourceSets.main.get().output)
        dependsOn(configurations.runtimeClasspath)
        from({ configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }.map { zipTree(it) } })

        manifest {
            attributes(
                "Main-Class" to application.mainClass,
            )
        }
    }
}