- clear m/c cache - ```sudo yum clean all```
- clear log files on / folder occupying more space with  ```du -d 0 -h *``` and run this daily with cron job like /backup_restore/backup_hive.sh
- check space left on m/c - ```df -h```
- Clear .hidden files within /home/ergo*/user workspace 
    - ```du -sch .[!.]* * |sort -h``` gives the storage space of hidden files 
- ```du -d 0 -h *``` on / folder to see which folders are occupying more space.
