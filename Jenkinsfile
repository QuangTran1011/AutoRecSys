pipeline {
  agent {
    kubernetes {
      yaml """
apiVersion: v1
kind: Pod
spec:
  serviceAccountName: jenkins-deployer
  restartPolicy: Never
  containers:
  - name: cicd
    image: quangtran1011/jenkins-agent:latest
    command: ['cat']
    tty: true
    env:
    - name: MLFLOW_TRACKING_URI
      valueFrom:
        configMapKeyRef:
          name: mlflow-config
          key: tracking_uri
"""
    }
  }

  options {
    disableConcurrentBuilds()
    timestamps()
  }

  parameters {
    booleanParam(
      name: 'RUN_DEPLOY',
      defaultValue: false,
      description: 'Set true to run Deploy stage only'
    )
  }

  environment {
    IMAGE_NAME = "quangtran1011/training_pipeline"
    GCS_BUCKET = "gs://kltn--data/config"
    MLFLOW_TRACKING_URI = "http://34.69.242.168:8080"
    INFER_SERVICE_YAML = "deployment/k8s/model_server/ranker-inferenceservice.yaml"
    REBUILD_IMAGE = "false"
  }

  stages {

    /* =========================
       1. Detect changes
       ========================= */
    stage('Detect changes') {
      when {
        allOf {
          expression { !params.RUN_DEPLOY }
          changeset "training_pipeline/src/**"
        }
      }
      steps {
        script {
          env.REBUILD_IMAGE = "true"
          echo "REBUILD_IMAGE = true"
        }
      }
    }

    /* =========================
       2. Build & Push image (conditional)
       ========================= */
    stage('Build & Push image') {
      when {
        allOf {
          expression { !params.RUN_DEPLOY }
          expression { env.REBUILD_IMAGE == "true" }
        }
      }
      agent any
      steps {
        script {
          checkout scm
          echo "Building Docker image..."
          def versionTag = "${BUILD_NUMBER}"
          def img = docker.build("${IMAGE_NAME}:${versionTag}", "training_pipeline")

          docker.withRegistry('', 'dockerhub-credential') {
            img.push(versionTag)
            img.push('latest')
          }
        }
      }
    }

    /* =========================
       3. Generate Kubeflow pipeline YAML
       ========================= */
    stage('Deploy pipeline') {
      when { expression { !params.RUN_DEPLOY } }
      steps {
        container('cicd') {
          sh 'python3 training_pipeline/training_pipeline.py'
          sh 'gsutil cp training_pipeline/pipeline/*.yaml ${GCS_BUCKET}/'
          sh 'python3 training_pipeline/pipeline/run_pipeline.py'
        }
      }
    }

    /* =========================
       4. Deploy model (MLflow gated)
       ========================= */
    stage('Deploy model') {
      when { expression { params.RUN_DEPLOY } }
      steps {
        container('cicd') {
          sh 'python3 training_pipeline/check_deploy_new_model.py'
        }
      }
    }
  }

  post {
    success {
      echo "CI/CD pipeline finished successfully"
    }
    failure {
      echo "CI/CD pipeline failed"
    }
  }
}
