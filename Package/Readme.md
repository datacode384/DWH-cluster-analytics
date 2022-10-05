# Nexus_deployment

- Nexus repo details - https://dth03.ibmgcloud.net/confluence/pages/viewpage.action?spaceKey=CCCI&title=Nexus+How+Tos#

- Manual upload - ```curl -v -u username:password -X POST 'https://sys0002.dev.ergo.liferunoffinsuranceplatform.com/repository/' -F "file=@dwh-python-scripts-1.0.tar.gz"```

- UI upload - https://sys0002.dev.ergo.liferunoffinsuranceplatform.com/#browse/upload:lip-raw-installation

# Packaging Python Application 

- Pre-requisites beofre wheel packaging a python project
    - If a folder is not a recognized python package, Create empty __init__.py in each sub-directory to make it as python package. Then only, find_pacakges() can find it as package withn our setup.py script and include this package for packaging 
    - Keep requirements.txt in folder where setup.py, package.py, MANIFEST.in files are present. 
    - Include / exclude files or depencencies in manifest.in file 
    - Activate venv 
    
- Build wheel  
    - Install setuptools, wheel 
        - pip install --upgrade setuptools wheel
        - pip install --upgrade tqdm twine 
        - python setup.py package
        - cd dist
        - chmod 777 *.gz (this is the wheel archieve we uploaded to nexus)
        
- Unpack wheel (or) install package
    - Move to dist/*.gz folder (or) pull from repo
    - Tar xzf *.gz
    - cd to untar folder
    - pip install -r requirements.txt  --no-index --find-links wheelhouse
    - go inside each one of the folders (db2, hive, pyspark) and follow readme.md in those folders to run the scripts

# Packaging Cognos Application

- Upload datenkatalog.zip to nexus

# Uploaded samples 
![Uploaded samples](/deployment/dwh-nexus-space.png)
