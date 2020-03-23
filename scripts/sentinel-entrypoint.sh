#!/bin/bash
cat > /codis/sentinel.conf << EOF
port ${PORT:=26379}
EOF

exec "$@"
