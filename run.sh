mkdir -p /data01/redis

make

./admin/codis-server-admin_9222.sh start
./admin/codis-server-admin_9223.sh start
./admin/codis-server-admin_9224.sh start
./admin/codis-server-admin_9225.sh start

ps -aux |grep codis