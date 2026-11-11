-- OTA-Server 完整表结构（建表 + 字段注释，一个文件搞定）
--
-- 只存「重启之后还要」的东西。
-- 在线状态、连接表、超时分桶都在内存里，不进库 —— 重启重建反而准确。
--
-- 判据：重启后要恢复的进库，不要的进内存。
--
-- 适用：PostgreSQL 14+
--
-- 重建方式：
--   DROP TABLE IF EXISTS commands, device_upgrades, rollout_batches, rollouts, firmware, devices CASCADE;
--   然后重新执行本文件。

BEGIN;

-- 先删再建：这个文件是「从零重建」用的，不是增量迁移。
-- 注意：执行它会清掉现有数据。生产环境不要这么干。
DROP TABLE IF EXISTS commands, device_upgrades, rollout_batches, rollouts, firmware, devices CASCADE;

-- ---------------------------------------------------------------------------
-- devices：设备档案
-- 这台设备是什么、属于谁、现在跑哪个版本
-- ---------------------------------------------------------------------------
CREATE TABLE devices
(
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sn              text        NOT NULL UNIQUE,
    token_hash      text        NOT NULL DEFAULT '',
    model           text        NOT NULL,
    group_name      text        NOT NULL DEFAULT '',
    fw_version      text        NOT NULL DEFAULT '',
    attrs           jsonb       NOT NULL DEFAULT '{}'::jsonb,
    last_seen_at    timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE  devices                IS '设备档案：型号、分组、当前版本。在线状态不在这里。';
COMMENT ON COLUMN devices.id             IS '内部编号，自增。只在本库内有意义，不对设备暴露';
COMMENT ON COLUMN devices.sn             IS '设备序列号，全局唯一。网关靠它认人，不是数据库 id';
COMMENT ON COLUMN devices.token_hash     IS '设备接入凭证的 SHA-256（64 位小写十六进制）。只存哈希不存明文：库被拖走时明文 token 等于所有设备被接管。设备令牌是随机串，不需要慢哈希';
COMMENT ON COLUMN devices.model          IS '硬件型号，例如 SIM-200。决定这台设备能升哪些固件';
COMMENT ON COLUMN devices.group_name     IS '分组名，例如「华东产线」。升级批次按它圈设备';
COMMENT ON COLUMN devices.fw_version     IS '当前固件版本，例如 1.2.3。升级成功时由服务端更新';
COMMENT ON COLUMN devices.attrs          IS '设备上报的动态属性，结构不固定所以存 JSONB：{"cpu":"8核","mem_mb":4096}';
COMMENT ON COLUMN devices.last_seen_at   IS '最后一次见到这台设备的时间。注意：不等于在线，在线看内存连接表。由内存每 30 秒批量刷一次';
COMMENT ON COLUMN devices.created_at     IS '这台设备第一次上报的时间';
COMMENT ON COLUMN devices.updated_at     IS '这条记录最后一次被修改的时间，由应用维护';

CREATE INDEX idx_devices_group     ON devices (group_name);
CREATE INDEX idx_devices_model     ON devices (model);
CREATE INDEX idx_devices_last_seen ON devices (last_seen_at DESC NULLS LAST);


-- ---------------------------------------------------------------------------
-- firmware：固件包
-- 有哪些包、放哪、校验值是多少
-- ---------------------------------------------------------------------------
CREATE TABLE firmware
(
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version     text        NOT NULL,
    model       text        NOT NULL,
    size_bytes  bigint      NOT NULL CHECK (size_bytes > 0),
    sha256      char(64)    NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    url         text        NOT NULL,
    notes       text        NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now(),

    UNIQUE (model, version)
);

COMMENT ON TABLE  firmware            IS '固件包索引。文件本身在对象存储，这里只存地址和校验值。';
COMMENT ON COLUMN firmware.id         IS '内部编号，自增';
COMMENT ON COLUMN firmware.version    IS '固件版本号，例如 1.2.3。和型号组合起来唯一';
COMMENT ON COLUMN firmware.model      IS '适用型号。一个版本只对一种型号有效';
COMMENT ON COLUMN firmware.size_bytes IS '文件大小，单位字节。设备下载完拿它和实际大小对一遍';
COMMENT ON COLUMN firmware.sha256     IS '文件 SHA-256，64 位小写十六进制。设备下载后自己校验，服务端不转发文件';
COMMENT ON COLUMN firmware.url        IS '下载地址，指向对象存储/CDN。业务服务器只发这个地址，不传文件本体';
COMMENT ON COLUMN firmware.notes      IS '发布说明，给人看的';
COMMENT ON COLUMN firmware.created_at IS '这个包入库的时间';


-- ---------------------------------------------------------------------------
-- rollouts：一次升级行动
-- 把哪个固件推给哪个分组、分几批、成功率多少就中止
-- ---------------------------------------------------------------------------
CREATE TABLE rollouts
(
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    firmware_id         bigint        NOT NULL REFERENCES firmware (id),
    group_name          text          NOT NULL,
    status              text          NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'running', 'paused', 'succeeded', 'failed', 'cancelled')),
    batch_plan          int[]         NOT NULL DEFAULT '{1,10,50,100}',
    success_threshold   numeric(5, 2) NOT NULL DEFAULT 95.00
                        CHECK (success_threshold BETWEEN 0 AND 100),
    created_at          timestamptz   NOT NULL DEFAULT now(),
    started_at          timestamptz,
    finished_at         timestamptz
);

COMMENT ON TABLE  rollouts                    IS '一次升级行动：固件 + 目标分组 + 分批策略 + 成功率阈值。';
COMMENT ON COLUMN rollouts.id                 IS '内部编号，自增';
COMMENT ON COLUMN rollouts.firmware_id        IS '要推的固件，指向 firmware.id';
COMMENT ON COLUMN rollouts.group_name         IS '目标分组。这次行动推给 devices.group_name 等于它的设备';
COMMENT ON COLUMN rollouts.status             IS '任务状态：pending 待开始 / running 进行中 / paused 已暂停 / succeeded 全部成功 / failed 失败 / cancelled 人工取消';
COMMENT ON COLUMN rollouts.batch_plan         IS '分批百分比，例如 {1,10,50,100}：先推 1%，达标再推 10%，以此类推';
COMMENT ON COLUMN rollouts.success_threshold  IS '成功率阈值，百分比。某一批低于它会自动中止，防止把整批设备刷成砖';
COMMENT ON COLUMN rollouts.created_at         IS '任务创建时间';
COMMENT ON COLUMN rollouts.started_at         IS '任务真正开始推的时间，还没开始为空';
COMMENT ON COLUMN rollouts.finished_at        IS '任务结束时间，成功、失败、取消都会填';

CREATE INDEX idx_rollouts_status ON rollouts (status);
CREATE INDEX idx_rollouts_fw     ON rollouts (firmware_id);


-- ---------------------------------------------------------------------------
-- rollout_batches：分批进度
-- 这次行动分了几批，每批推到哪一步
-- ---------------------------------------------------------------------------
CREATE TABLE rollout_batches
(
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rollout_id      bigint      NOT NULL REFERENCES rollouts (id) ON DELETE CASCADE,
    seq             int         NOT NULL,
    percent         int         NOT NULL CHECK (percent BETWEEN 1 AND 100),
    status          text        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'skipped')),
    target_count    int         NOT NULL DEFAULT 0,
    success_count   int         NOT NULL DEFAULT 0,
    fail_count      int         NOT NULL DEFAULT 0,
    started_at      timestamptz,
    finished_at     timestamptz,

    UNIQUE (rollout_id, seq)
);

COMMENT ON TABLE  rollout_batches                IS '升级任务的分批进度。';
COMMENT ON COLUMN rollout_batches.id             IS '内部编号，自增';
COMMENT ON COLUMN rollout_batches.rollout_id     IS '属于哪一次升级行动，指向 rollouts.id';
COMMENT ON COLUMN rollout_batches.seq            IS '第几批，从 1 开始。同一次行动里不重复';
COMMENT ON COLUMN rollout_batches.percent        IS '这一批占目标设备的百分比，1 到 100';
COMMENT ON COLUMN rollout_batches.status         IS '这一批的状态：pending 未开始 / running 推送中 / succeeded 达标 / failed 未达标 / skipped 跳过';
COMMENT ON COLUMN rollout_batches.target_count   IS '这一批圈中多少台设备';
COMMENT ON COLUMN rollout_batches.success_count  IS '这一批里升级成功的台数';
COMMENT ON COLUMN rollout_batches.fail_count     IS '这一批里升级失败的台数';
COMMENT ON COLUMN rollout_batches.started_at     IS '这一批开始推送的时间';
COMMENT ON COLUMN rollout_batches.finished_at    IS '这一批结束的时间';

CREATE INDEX idx_batches_rollout ON rollout_batches (rollout_id, status);


-- ---------------------------------------------------------------------------
-- device_upgrades：每台设备的结果
-- 从哪个版本升到哪个版本，成没成
-- ---------------------------------------------------------------------------
CREATE TABLE device_upgrades
(
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rollout_id      bigint      NOT NULL REFERENCES rollouts (id) ON DELETE CASCADE,
    device_id       bigint      NOT NULL REFERENCES devices (id) ON DELETE CASCADE,
    from_version    text        NOT NULL,
    to_version      text        NOT NULL,
    status          text        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'notified', 'downloading', 'installing', 'succeeded', 'failed')),
    percent         int         NOT NULL DEFAULT 0 CHECK (percent BETWEEN 0 AND 100),
    error           text        NOT NULL DEFAULT '',
    updated_at      timestamptz NOT NULL DEFAULT now(),

    UNIQUE (rollout_id, device_id)
);

COMMENT ON TABLE  device_upgrades                 IS '每台设备在一次升级行动里的结果。';
COMMENT ON COLUMN device_upgrades.id              IS '内部编号，自增';
COMMENT ON COLUMN device_upgrades.rollout_id      IS '属于哪一次升级行动，指向 rollouts.id';
COMMENT ON COLUMN device_upgrades.device_id       IS '是哪台设备，指向 devices.id';
COMMENT ON COLUMN device_upgrades.from_version    IS '升级前的版本。回滚时用它知道该退回到哪';
COMMENT ON COLUMN device_upgrades.to_version      IS '这次要升到的目标版本';
COMMENT ON COLUMN device_upgrades.status          IS '单台设备的状态：pending 待通知 / notified 已通知 / downloading 下载中 / installing 安装中 / succeeded 成功 / failed 失败';
COMMENT ON COLUMN device_upgrades.percent         IS '下载或安装进度，0 到 100。设备上报多少就记多少';
COMMENT ON COLUMN device_upgrades.error           IS '失败原因，设备回执里带的说明。成功时为空';
COMMENT ON COLUMN device_upgrades.updated_at      IS '这台设备这条记录最后更新的时间';

CREATE INDEX idx_upgrades_rollout ON device_upgrades (rollout_id, status);
CREATE INDEX idx_upgrades_device  ON device_upgrades (device_id, updated_at DESC);


-- ---------------------------------------------------------------------------
-- commands：远程指令
-- 下发了什么，设备回了什么
-- ---------------------------------------------------------------------------
CREATE TABLE commands
(
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    command_id  text        NOT NULL UNIQUE,
    device_id   bigint      NOT NULL REFERENCES devices (id) ON DELETE CASCADE,
    type        text        NOT NULL,
    payload     jsonb       NOT NULL DEFAULT '{}'::jsonb,
    status      text        NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'sent', 'acked', 'failed', 'timeout')),
    result      text        NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now(),
    acked_at    timestamptz
);

COMMENT ON TABLE  commands             IS '远程指令与回执。设备离线时指令留在这里，等它上线再投。';
COMMENT ON COLUMN commands.id          IS '内部编号，自增';
COMMENT ON COLUMN commands.command_id  IS '业务侧的指令编号，全局唯一。幂等键：同一个 id 重复下发，设备只执行一次';
COMMENT ON COLUMN commands.device_id   IS '发给哪台设备，指向 devices.id';
COMMENT ON COLUMN commands.type        IS '指令类型：reboot 重启 / clear_cache 清缓存 / set_config 改配置 / pull_log 拉日志';
COMMENT ON COLUMN commands.payload     IS '指令参数，JSON。不同 type 的结构不同';
COMMENT ON COLUMN commands.status      IS '指令状态：pending 待下发 / sent 已下发 / acked 设备已回执 / failed 执行失败 / timeout 超时未回';
COMMENT ON COLUMN commands.result      IS '设备回执里的说明文字，失败原因也放这里';
COMMENT ON COLUMN commands.created_at  IS '指令创建时间';
COMMENT ON COLUMN commands.acked_at    IS '设备回执的时间，还没回为空';

CREATE INDEX idx_commands_device ON commands (device_id, status);
CREATE INDEX idx_commands_status ON commands (status, created_at);

COMMIT;
