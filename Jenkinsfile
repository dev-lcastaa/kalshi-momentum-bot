// Jenkins on node002 coordinates deployments. Agents: label 'node001' and label 'node003'.
// Images are built on the node that runs them (both aarch64), so no registry is needed.
// Order matters: the node003 collector uploads to node001 ingest, so node001 must be ready first.

def onMain() {
    def b = env.BRANCH_NAME ?: env.GIT_BRANCH ?: ''
    return b == 'main' || b == 'origin/main'
}

pipeline {
    agent none
    environment {
        DOCKER_CONTEXT = 'default'
    }
    options {
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '30'))
        timeout(time: 45, unit: 'MINUTES')
    }
    parameters {
        booleanParam(name: 'DEPLOY', defaultValue: true, description: 'Deploy after tests pass (main branch only)')
    }
    triggers {
        githubPush()
        pollSCM('H/5 * * * *')
    }
    stages {
        stage('Test') {
            parallel {
                stage('Python') {
                    agent { label 'node001' }
                    steps {
                        // Full pytest suite + ruff (all packages share one source tree).
                        sh 'docker build -f services/ingest/Dockerfile --target test -t kmb-ingest:test .'
                    }
                }
                stage('UI') {
                    agent { label 'node003' }
                    steps {
                        // tsc + vitest + vite build of frontend/.
                        sh 'docker build -f services/app/Dockerfile --target ui -t kmb-ui:test .'
                    }
                }
            }
        }
        stage('Deploy node001') {
            when { expression { params.DEPLOY && onMain() } }
            agent { label 'node001' }
            steps {
                sh 'bash deploy/node001/deploy.sh'
            }
        }
        stage('Deploy node003') {
            when { expression { params.DEPLOY && onMain() } }
            agent { label 'node003' }
            steps {
                sh 'bash deploy/node003/deploy.sh'
            }
        }
    }
}
