FROM logstash:8.13.0

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s CMD curl --fail http://localhost:9600/_node/stats || exit 1