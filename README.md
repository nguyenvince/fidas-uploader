# Upload data from Fidas to CITIESair server

Add the .service
```
sudo nano /etc/systemd/system/fidas_uploader.service
```

```
[Unit]
Description=Fidas Uploader
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/bin/python3 /home/admin/fidas_uploader/upload_to_citiesair.py
WorkingDirectory=/home/admin/fidas_uploader
Restart=on-failure
RestartSec=2
User=admin

# kill old instance cleanly before restart
KillMode=mixed
TimeoutStopSec=5

[Install]
WantedBy=multi-user.target
```