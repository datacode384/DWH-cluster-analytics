# SSH connection for windows users

## key generating and adding process:
```
ssh-keygen -t rsa -b 4096 -C "your_email@example.com"
```
use the default path (~/.ssh) and enther the ssh pass you want

check to see if the ssh-agent is running:
```
get-service ssh-agent
```

add the ssk key to the agent:
```
ssh-add ~/.ssh/id_rsa
```

open id_rsa.pub under ~/.ssh and copy the contents, quick command for that:
```
notepad ~/id_rsa.pub
```
next, go to your github account settings, ssh and gpg keys and add new ssh key
give it a name and paste the key


in ~/.ssh create a new textdoc and name it 'config', without any extension and paste the following:
```
Host *
    AddKeysToAgent yes
    IgnoreUnknown AddKeysToAgent,UseKeychain
    UseKeychain yes
    User [username]
    IdentityFile ~/.ssh/id_rsa
```

make sure you change the username to avoid connectivity issues

### note: to copy-paste something from your machine into the VM ssh terminal on PowerShell you need to paste by right-clicking the terminal.

go to : https://../vault/secrets/ssh-client-signer/list  and navigate to your user. copy the contents of the public key in the ~/.ssh folder and paste them in the field. in the case you don't have access to the link, make sure you are correctly logged in (with your user) and if you still have issues regarding permissions, contact the devops.

save the generated key in the ~/.ssh folder with the name 'id_rsa-cert.pub'. (Note: 
For security purposes, the vault certificates are refreshed every 3 days (Hence, the new vault certificate has to be copied to id_rsa_signed with in your system .ssh folder every 3 days)

next you have to restart the ssh-agent process:
```
Set-Service -Name ssh-agent -StartupType Manual
stop-service ssh-agent
```
make sure it stopped:
```
get-service ssh-agent
start-service ssh-agent
```

redo the identities:
```
ssh-add -D
ssh-add -k /Users/[local_username]/.ssh/id_rsa
ssh-add -l
```

# ssh-ing to the VM
in order to ssh, make sure you are connected to the vpn
first time you connect through ssh to the machine use the following command to make sure that a folder with your name is created on the VM:
```
ssh -i /Users/[local_username]/.ssh/id_rsa_signed [username]@sys1007.dev.ergo.liferunoffinsuranceplatform.com -p 122
```
now whenever you want to connect, you can use direcly ```ssh [ip]```

in the VM, in order to use git, redo the key generating process:
```
ssh-keygen -t rsa -b 4096 -C "your_email@example.com"
```
since this is a linux machine, you have to start the ssh-agent first:
```
eval $(ssh-agent -s)
```
```
ssh-add ~/.ssh/id_rsa
```

check that your correct git account is correctly stored on the VM:
```
git config --global --list
```
# sftp
in order to check that sftp works correctly, we're going to send the id_rsa.pub key from the VM machine to our machine, to use sftp, you need to run a powershell terminal as administrator, make sure you navigate to a folder to and from where you want to send files to and from the VM, then:
```
sftp [ip]
```
```get [path]/filename``` grabs the file from the VM and copies it into the folder from where you run the command

```put filename``` sends the file to the home directory on the VM

after you initialize the sftp, you will want to do:
```
get .ssh/id_rsa.pub
```
now you're ready to push/pull to and from git on the VM as well

