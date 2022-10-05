Note: This package is tested with cloudera cdh 6.3.2 installed on RHEL7. Mimic similar installation settings.

1. tar xzf *.gz
2. cd to hive folder
3. source [bashrc](/venv/bashrc), [bash_profile](/venv/bash_profile)
3. Run ./run_commands.sh

# SIT Specific smoke test run instructions
- As SIT installation has kerberos authentication enabled , For running scripts in Hive folder, use hive shell as beeline needs kreberos authentication credentials
