# SSH Connection for linux users

## Generate ssh keys in your local machine
1. ssh-keygen -t rsa -b 4096 -C "your_email@example.com"
2. Edit config line and add below lines to /.ssh/config

``` 
Host *
        AddKeysToAgent yes
        UseKeychain yes
        User kdamarla
        IdentityFile ~/.ssh/id_rsa
```

3. Start SSH-Agent 

```
killall ssh-agent; eval `ssh-agent`
ssh-add -D 
ssh-add -K /Users/krishna/.ssh/id_rsa
ssh-add -l 
```

Ref: https://matt.ucc.asn.au/ssh-xfer/

4. For every new vault certificate generation, refresh ssh-agent
5. Restart ssh agent	


# setup vault pub key 

- go to https:/.../ui/vault/secrets/ssh-client-signer/list and navigate to your user. 
- copy the contents of the public key in the ~/.ssh folder and paste them in the field. 
- save the generated key in the ~/.ssh folder with the name 'id_rsa-cert.pub'

# SSH connection test to itergo virtual machines
- After you configure the Vault certificate, connected to itergo openvpn and generated ssh keys in our local mahcine, ssh login with your credentials for testing login to itergo virtual machines 

- Server 1 ssh  (Cognos machine)
	- ```ssh machine``` (If ssh-agent installed)

- Server 2 ssh (Cloudera machine)

- db2 -ssh 

- DIP connection - ssh

Note: 
For security purposes, the vault certificates are refreshed every 3 days (Hence, the new vault certificate has to be copied to id_rsa_signed with in your system .ssh folder every 3 days) 

# SFTP 

- For simple file transfer from local to dwh machines, we can use sftp. It is highly recommended to use git for file transfers.

- If you configured ssh-agent, sftp to machine to get files to & from vm machines
	- get filename
	- put filename

