import { config } from "./config";
import { CephApi } from "./services/ceph-api";
import { CephController } from "./controllers/ceph.controller";
import { AmqpService } from "./services/amqp";
import { TasksController } from "./controllers/tasks.controller";
import logger from "./lib/logger";

async function main(): Promise<void> {
  const amqp = new AmqpService();
  await amqp.init();

  const cephApi = new CephApi({
    baseUrl: config.ceph.url,
    username: config.ceph.user,
    password: config.ceph.password,
    allowInsecureTls: config.ceph.insecureTls,
    acceptHeader: config.ceph.apiAccept,
  });

  const cephController = new CephController(cephApi, config.ceph.defaultPool);
  const tasksController = new TasksController(cephController, amqp, config.broker.service.name);

  try {


    logger.info("Fetching iSCSI targets...");
    const targets = await cephApi.getTargets();
    logger.info("iSCSI targets response", { targets });
    logger.info("iSCSI targets (raw JSON)", { targetsJson: JSON.stringify(targets, null, 2) });

    const targetList = Array.isArray(targets) ? targets : [];
    const firstTarget = targetList[0];
    const firstIqn = firstTarget?.target_iqn ?? firstTarget?.iqn;
    if (firstIqn) {
      const targetDetail = await cephApi.rawRequest(
        "GET",
        `/api/iscsi/target/${encodeURIComponent(firstIqn)}`
      );
      logger.info("iSCSI target detail response", { data: targetDetail.data });
      logger.info("iSCSI target detail (raw JSON)", {
        targetDetailJson: JSON.stringify(targetDetail.data, null, 2),
      });
    }
  } catch (err: any) {
    if (err.response) {
      logger.error(`Error ${err.response.status}`, { data: err.response.data });
    } else {
      logger.error(err.message || String(err), { err });
    }
    process.exit(1);
  } finally {
    await amqp.close();
  }
}

void main();
