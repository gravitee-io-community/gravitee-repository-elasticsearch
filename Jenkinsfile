node() {
    def elasticHost = "-DELASTIC_HOST=${env.ELASTIC_TEST_HOST}"
    def elasticPort = "-DELASTIC_PORT=${env.ELASTIC_TEST_PORT}"
    def mvnArgs = ["-U", "-Pgravitee-report", elasticHost, elasticPort, "clean", "deploy"]

    stage "Checkout"
    checkout scm

    stage "Build"

    def mvnHome = tool 'MVN33'
    def javaHome = tool 'JDK 8'
    def nodeHome = tool 'NodeJS 0.12.4'
    withEnv(["PATH+MAVEN=${mvnHome}/bin",
             "PATH+NODE=${nodeHome}/bin",
             "JAVA_HOME=${javaHome}"]) {
        def mvnCommamd = ["${mvnHome}/bin/mvn"] + mvnArgs
        sh "${mvnCommamd.join(" ")}"
        try {
            sh "ls **/target/surefire-reports/TEST-*.xml"
            step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])
        } catch (Exception ex) {
            echo "No tests to archive"
        }
        try {
            sh "ls target/surefire-reports/TEST-*.xml"
            step([$class: 'JUnitResultArchiver', testResults: 'target/surefire-reports/TEST-*.xml'])
        } catch (Exception ex) {
            echo "No tests to archive"
        }

        stage("SonarQube analysis") {
            withSonarQubeEnv('SonarQube') {
                sh "${mvnHome}/bin/mvn sonar:sonar"
            }
        }
    }
}
