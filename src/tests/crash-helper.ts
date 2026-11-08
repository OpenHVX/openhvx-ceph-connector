import { OperationJournal } from '../services/operations/journal';
import { Command } from '../services/operations/protocol';
import { openSync, writeSync, fsyncSync, closeSync } from 'node:fs';
async function main() {
  const journal = new OperationJournal();
  await journal.open(process.env.CRASH_MONGO_URL!);
  const command = JSON.parse(process.env.CRASH_COMMAND!) as Command;
  await journal.execute(command, async () => {
    const fd = openSync(process.env.CRASH_EFFECT_PATH!, 'a');
    writeSync(fd, 'effect\n'); fsyncSync(fd); closeSync(fd);
    process.exit(42);
  });
}
void main().catch(() => process.exit(1));
