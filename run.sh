mkdir -p /data01/redis

kill `lsof -t -i:9222`
kill `lsof -t -i:9223`
kill `lsof -t -i:9224`
kill `lsof -t -i:9225`
kill `lsof -t -i:9226`

make

rm -rf /tmp/redis_9222.pid
rm -rf /tmp/redis_9223.pid
rm -rf /tmp/redis_9224.pid
rm -rf /tmp/redis_9225.pid
rm -rf /tmp/redis_9226.pid

mkdir -p /data01/redis/9222
mkdir -p /data01/redis/9223
mkdir -p /data01/redis/9224
mkdir -p /data01/redis/9225
mkdir -p /data01/redis/9226

./admin/codis-server-admin_9222.sh start
./admin/codis-server-admin_9223.sh start
./admin/codis-server-admin_9224.sh start
./admin/codis-server-admin_9225.sh start
./admin/codis-server-admin_9226.sh start

ps -aux |grep codis