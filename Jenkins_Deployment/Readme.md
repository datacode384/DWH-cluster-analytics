# Jenkins Build related instructions
- Jenkins ui - https://sys0001.dev.ergo.liferunoffinsuranceplatform.com/
- Jenkinfile samples taken from https://github.ibmgcloud.net/Ergo-Platform-Life-Runoff/lip-core-devops-playground
  - To download an artifact, keep the content inside Jenkinsfile_download to Jenkinsfile and see build steps in jenkins as below ![Jenkins_UI](/Jenkins_UI.png)
  - To upload an artifact with a stage of Download & Zip prcessing, keep the content inside Jenkinsfile_upload to Jenkinsfile 
  - For retrieving github version/code,  keep the content inside Jenkinsfile_github to Jenkinsfile
- Just for info: When application is loosely coupled with docker containers, jenkins build with yaml files
  - sample : https://github.ibmgcloud.net/Ergo-Platform-Life-Runoff/lip-core-devops-playground/blob/master/helm/lip-core-test-playground/values.sit.yaml

