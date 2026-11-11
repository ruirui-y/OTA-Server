-- 造测试设备：5 万台，sn 和 token 有规律，模拟器照这个规律发。
--
--   sn    = sim-000000 ... sim-049999
--   token = tok-000000 ... tok-049999
--
-- 库里只存 token 的 SHA-256，明文由模拟器自己算，不上网。
--
-- 用法：
--   psql -h 127.0.0.1 -U dev -d ota_server -f scripts/seed_devices.sql
--
-- 重复执行不会报错（ON CONFLICT DO NOTHING）。

INSERT INTO devices (sn, token_hash, model, group_name, fw_version)
SELECT
    'sim-' || lpad(i::text, 6, '0'),
    encode(sha256(('tok-' || lpad(i::text, 6, '0'))::bytea), 'hex'),
    'SIM-200',
    'bench',
    '1.0.0'
FROM generate_series(0, 49999) AS i
ON CONFLICT (sn) DO NOTHING;

SELECT count(*) AS devices FROM devices;