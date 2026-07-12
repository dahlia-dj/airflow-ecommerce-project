// Jenkinsfile
// -----------
// Pipeline CI/CD du projet "ecommerce-sales-pipeline".
// Realise le checkout du code, l'installation des dependances, les tests,
// la validation et le deploiement du DAG Airflow, puis le declenchement
// du DAG et la verification du stockage MongoDB.

pipeline {
    agent any

    environment {
        PROJECT_DIR       = "${WORKSPACE}"
        VENV_DIR          = "${WORKSPACE}/.venv"
        AIRFLOW_DAGS_DIR  = "/opt/airflow/dags"
        AIRFLOW_HOME      = "/opt/airflow"
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
                echo "=== Installation des dependances Python ==="
                sh '''
                    python3 -m venv "${VENV_DIR}"
                    . "${VENV_DIR}/bin/activate"
                    pip install --upgrade pip
                    PYVER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
                    pip install --no-cache-dir -r requirements.txt --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-${PYVER}.txt
                   
                '''
            }
        }

        stage('Run tests') {
            steps {
                echo "=== Execution des tests unitaires avec pytest ==="
                sh '''
                    . "${VENV_DIR}/bin/activate"
                    pytest tests/ -v --junitxml=reports/test-results.xml
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
                echo "=== Validation syntaxique du DAG Airflow ==="
                sh '''
                    . "${VENV_DIR}/bin/activate"
                    python -m py_compile dags/*.py
                '''
            }
        }

        stage('Deploy DAG') {
            steps {
                echo "=== Deploiement du DAG vers Airflow ==="
                sh '''
                     docker cp dags/ecommerce_sales_pipeline.py "airflow_webserver:/opt/airflow/dags/"
                     echo "DAG deployé dans airflow_webserver:/opt/airflow/dags/"
                '''
            }
        }

        stage('Trigger DAG') {
            steps {
                echo "=== Declenchement du DAG Airflow ==="
                sh '''
                    docker exec airflow_webserver airflow dags trigger ecommerce_sales_pipeline
                '''
            }
        }

        stage('Verify MongoDB') {
            steps {
                echo "=== Verification des donnees stockees dans MongoDB ==="
                sh '''
                    . "${VENV_DIR}/bin/activate"
                    python scripts/check_mongodb.py --uri "${MONGO_URI}"
                '''
            }
        }
    }

    post {
        success {
            echo "Pipeline exécute avec succès : DAG déployé, déclenché et données vérifiées dans MongoDB."
        }
        failure {
            echo "Le pipeline a échoue. Consulter les logs des stages ci-dessus pour diagnostiquer."
        }
        always {
            cleanWs()
        }
    }
}
