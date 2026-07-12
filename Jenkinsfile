
pipeline {
    agent any

    environment {
        AIRFLOW_IMAGE     = "ecommerce-airflow:latest"
        AIRFLOW_CONTAINER = "airflow_webserver"
        MONGO_URI         = "mongodb://mongodb:27017"
        DAG_ID            = "ecommerce_sales_pipeline"
    }

    options {
        timestamps()
        buildDiscarder(logRotator(numToKeepStr: '20'))
        disableConcurrentBuilds()
    }

    stages {

        stage('Checkout') {
            steps {
                echo "=== Recuperation du code source depuis Git ==="
                checkout scm
                sh 'git log -1 --oneline'
            }
        }

        stage('Install dependencies') {
            steps {
                echo "=== Verification des dependances Python (image ${AIRFLOW_IMAGE}) ==="
                sh '''
                    CID=$(docker create -w /tmp -e PYTHONDONTWRITEBYTECODE=1 --entrypoint bash "${AIRFLOW_IMAGE}" \
                        -c "pip install -r requirements.txt")
                    docker cp requirements.txt "${CID}":/tmp/requirements.txt
                    docker start -a "${CID}"
                    docker rm -f "${CID}"
                '''
            }
        }

        stage('Run tests') {
            steps {
                echo "=== Execution des tests unitaires avec pytest (image ${AIRFLOW_IMAGE}) ==="
                sh '''
                    mkdir -p reports
                    CID=$(docker create -w /tmp -e PYTHONDONTWRITEBYTECODE=1 --entrypoint bash "${AIRFLOW_IMAGE}" \
                        -c "pytest tests/ -v --junitxml=reports/test-results.xml")
                    docker cp dags "${CID}":/tmp/dags
                    docker cp tests "${CID}":/tmp/tests
                    docker cp requirements.txt "${CID}":/tmp/requirements.txt
                    docker start -a "${CID}"
                    docker cp "${CID}":/tmp/reports/test-results.xml reports/test-results.xml
                    docker rm -f "${CID}"
                '''
            }
            post {
                always {
                    junit 'reports/test-results.xml'
                }
            }
        }

        stage('Validate DAG') {
            steps {
                echo "=== Validation syntaxique du DAG Airflow (image ${AIRFLOW_IMAGE}) ==="
                sh '''
                    CID=$(docker create -w /tmp -e PYTHONDONTWRITEBYTECODE=1 --entrypoint bash "${AIRFLOW_IMAGE}" \
                        -c "python -m py_compile dags/*.py")
                    docker cp dags "${CID}":/tmp/dags
                    docker start -a "${CID}"
                    docker rm -f "${CID}"
                '''
            }
        }

        stage('Deploy DAG') {
            steps {
                echo "=== Deploiement du DAG vers le conteneur Airflow ==="
                sh '''
                    docker cp dags/ecommerce_sales_pipeline.py "${AIRFLOW_CONTAINER}:/opt/airflow/dags/"
                    echo "DAG deploye dans ${AIRFLOW_CONTAINER}:/opt/airflow/dags/"
                '''
            }
        }

        stage('Trigger DAG') {
            steps {
                echo "=== Declenchement du DAG Airflow ==="
                sh 'docker exec -T "${AIRFLOW_CONTAINER}" airflow dags trigger "${DAG_ID}"'
            }
        }

        stage('Verify MongoDB') {
            steps {
                echo "=== Verification des donnees stockees dans MongoDB ==="
                sh '''
                    docker cp scripts/check_mongodb.py "${AIRFLOW_CONTAINER}:/opt/airflow/scripts/check_mongodb.py"
                    docker exec -T "${AIRFLOW_CONTAINER}" python /opt/airflow/scripts/check_mongodb.py --uri "${MONGO_URI}"
                '''
            }
        }
    }

    post {
        success {
            echo "Pipeline execute avec succes : DAG deploye, declenche et donnees verifiees dans MongoDB."
        }
        failure {
            echo "Le pipeline a echoue. Consulter les logs des stages ci-dessus pour diagnostiquer."
        }
        always {
            cleanWs()
        }
    }
}