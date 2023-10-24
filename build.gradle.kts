plugins {
    id("java")
    application
}

group = "com.github.aayushjn"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
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
        manifest {
            attributes("Main-Class" to application.mainClass)
        }
    }
}