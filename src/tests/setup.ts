process.env.RMQ_URL ||= 'amqp://integration:integration-only-password@localhost:5672/';
process.env.STORAGE_CONTROLLER_AGENT_ID ||= 'test-storage';
process.env.CEPH_API_URL ||= 'http://127.0.0.1:1';
process.env.CEPH_API_USER ||= 'test';
process.env.CEPH_API_PASSWORD ||= 'test-only-password';
process.env.CEPH_GW_ISCSI_HOST ||= 'gateway';
process.env.CEPH_GW_ISCSI_IP ||= '127.0.0.1';
process.env.LOG_ENABLED = '0';
