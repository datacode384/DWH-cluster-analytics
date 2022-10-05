Note: This package is tested with cloudera cdh 6.3.2 installed on RHEL7 with python 3.7.1(venv)
1. Follow [system settings](/deployment/smoke-tests-package/python/venv) 
2. unzip / untar (tar xzf *.gz) folder,  ```cd to unzipped / untared folder```, ```chmod -R 777 unzipped_or_untared_folder```
3. For zip archive, skip step 3. For tar archive, ```pip install -r requirements.txt --no-index --find-links wheelhouse```
4. Go inside each one of the folders (pyspark, db2, iws) and follow readme.md in those folders to run the scripts


