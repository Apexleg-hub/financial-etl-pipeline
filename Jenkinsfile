
pipeline {
    agent any
    
    environment {
        PYTHON_VERSION = '3.10'
        DOCKER_REGISTRY = 'your-registry'
        AWS_REGION = 'us-east-1'
    }
    
    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }
        
        stage('Setup Environment') {
            steps {
                sh '''
                python -m venv venv
                source venv/bin/activate
                pip install --upgrade pip
                pip install -r requirements.txt -r requirements-dev.txt
                '''
            }
        }
        
        stage('Lint') {
            steps {
                sh '''
                source venv/bin/activate
                black --check src tests
                flake8 src tests
                isort --check-only src tests
                mypy src
                '''
            }
        }
        
        stage('Unit Tests') {
            steps {
                sh '''
                source venv/bin/activate
                pytest tests/unit/ -v --junitxml=test-results.xml --cov=src --cov-report=xml
                '''
            }
            post {
                always {
                    junit 'test-results.xml'
                    publishCoverage adapters: [coberturaAdapter('coverage.xml')]
                }
            }
        }
        
        stage('Integration Tests') {
            steps {
                sh '''
                docker-compose -f docker-compose.test.yml up -d
                sleep 10
                source venv/bin/activate
                pytest tests/integration/ -v
                docker-compose -f docker-compose.test.yml down
                '''
            }
        }
        
        stage('Security Scan') {
            steps {
                sh '''
                source venv/bin/activate
                safety check -r requirements.txt
                bandit -r src -f json -o bandit-report.json
                '''
            }
        }
        
        stage('Build Docker Image') {
            when {
                branch 'main'
            }
            steps {
                script {
                    docker.withRegistry("https://${DOCKER_REGISTRY}", 'docker-credentials') {
                        def image = docker.build("${DOCKER_REGISTRY}/financial-etl-pipeline:${env.BUILD_ID}")
                        image.push()
                        image.push('latest')
                    }
                }
            }
        }
        
        stage('Deploy to Staging') {
            when {
                branch 'develop'
            }
            steps {
                withAWS(region: env.AWS_REGION, credentials: 'aws-credentials') {
                    sh '''
                    aws ecs update-service \
                      --cluster etl-staging-cluster \
                      --service etl-pipeline-service \
                      --force-new-deployment
                    '''
                }
            }
        }
        
        stage('Deploy to Production') {
            when {
                branch 'main'
            }
            input {
                message "Deploy to production?"
                ok "Yes, deploy"
            }
            steps {
                withAWS(region: env.AWS_REGION, credentials: 'aws-credentials') {
                    sh '''
                    aws ecs update-service \
                      --cluster etl-production-cluster \
                      --service etl-pipeline-service \
                      --force-new-deployment
                    '''
                }
            }
        }
    }
    
    post {
        always {
            cleanWs()
            emailext (
                subject: "${env.JOB_NAME} - Build #${env.BUILD_NUMBER} - ${currentBuild.currentResult}",
                body: """Check console output at ${env.BUILD_URL}""",
                to: 'team@example.com'
            )
        }
        success {
            slackSend(
                channel: '#deployments',
                color: 'good',
                message: "${env.JOB_NAME} - Build #${env.BUILD_NUMBER} succeeded"
            )
        }
        failure {
            slackSend(
                channel: '#deployments',
                color: 'danger',
                message: " ${env.JOB_NAME} - Build #${env.BUILD_NUMBER} failed"
            )
        }
    }
}
