import path from 'path';
import fs from 'fs';
import { createRequire } from 'module';
import { Worker } from 'worker_threads';

const require = createRequire(import.meta.url);

// Database path setting (can be overridden for testing via process.env.DB_PATH)
const isVercel = Boolean(process.env.VERCEL || process.env.AWS_LAMBDA_FUNCTION_NAME);
const defaultDbPath = isVercel
  ? '/tmp/queuecraft.db'
  : path.join(process.env.INIT_CWD || process.cwd(), 'queuecraft.db');

const dbPath = process.env.DB_PATH || defaultDbPath;

let dbInstance: any = null;

function createWorkerSqlJsDb(targetPath: string): any {
  let worker: Worker | null = null;
  let sharedBuf: Int32Array | null = null;
  let responseBuffer: any = null;

  function initWorker() {
    if (worker) return;

    const workerCode = `
      import { parentPort } from 'worker_threads';
      import initSqlJs from 'sql.js';
      import fs from 'fs';
      import path from 'path';

      let SQL;
      let db;
      const targetPath = ${JSON.stringify(targetPath)};

      function saveDisk() {
        if (targetPath && targetPath !== ':memory:') {
          try {
            const dbDir = path.dirname(targetPath);
            if (!fs.existsSync(dbDir)) {
              fs.mkdirSync(dbDir, { recursive: true });
            }
            const data = db.export();
            fs.writeFileSync(targetPath, Buffer.from(data));
          } catch (e) {}
        }
      }

      async function init() {
        SQL = await initSqlJs();
        if (targetPath && targetPath !== ':memory:' && fs.existsSync(targetPath)) {
          try {
            const filebuffer = fs.readFileSync(targetPath);
            db = new SQL.Database(filebuffer);
          } catch (e) {
            db = new SQL.Database();
          }
        } else {
          db = new SQL.Database();
        }
        parentPort.postMessage({ type: 'INIT_DONE' });
      }

      parentPort.on('message', (msg) => {
        if (msg.type === 'INIT') {
          init();
        } else if (msg.type === 'EXEC') {
          try {
            db.exec(msg.sql);
            saveDisk();
            parentPort.postMessage({ type: 'RES', result: true });
          } catch (err) {
            parentPort.postMessage({ type: 'RES', error: err.message });
          }
        } else if (msg.type === 'PREPARE_GET') {
          try {
            const stmt = db.prepare(msg.sql);
            const hasRow = stmt.step(msg.params || []);
            const res = hasRow ? stmt.getAsObject() : undefined;
            stmt.free();
            parentPort.postMessage({ type: 'RES', result: res });
          } catch (err) {
            parentPort.postMessage({ type: 'RES', error: err.message });
          }
        } else if (msg.type === 'PREPARE_ALL') {
          try {
            const stmt = db.prepare(msg.sql);
            stmt.bind(msg.params || []);
            const results = [];
            while (stmt.step()) {
              results.push(stmt.getAsObject());
            }
            stmt.free();
            parentPort.postMessage({ type: 'RES', result: results });
          } catch (err) {
            parentPort.postMessage({ type: 'RES', error: err.message });
          }
        } else if (msg.type === 'PREPARE_RUN') {
          try {
            const stmt = db.prepare(msg.sql);
            stmt.run(msg.params || []);
            stmt.free();
            let changes = 0;
            let lastInsertRowid = 0;
            try {
              const resC = db.exec('SELECT changes()');
              if (resC.length && resC[0].values.length) changes = resC[0].values[0][0];
              const resId = db.exec('SELECT last_insert_rowid()');
              if (resId.length && resId[0].values.length) lastInsertRowid = resId[0].values[0][0];
            } catch (e) {}
            saveDisk();
            parentPort.postMessage({ type: 'RES', result: { changes, lastInsertRowid } });
          } catch (err) {
            parentPort.postMessage({ type: 'RES', error: err.message });
          }
        } else if (msg.type === 'CLOSE') {
          saveDisk();
          try { db.close(); } catch (e) {}
          parentPort.postMessage({ type: 'RES', result: true });
        }
      });
    `;

    sharedBuf = new Int32Array(new SharedArrayBuffer(4));
    worker = new Worker(workerCode, { eval: true } as any);

    worker.on('message', (msg) => {
      responseBuffer = msg;
      Atomics.store(sharedBuf!, 0, 1);
      Atomics.notify(sharedBuf!, 0);
    });

    Atomics.store(sharedBuf, 0, 0);
    worker.postMessage({ type: 'INIT' });
    Atomics.wait(sharedBuf, 0, 0, 5000);
  }

  function callSync(req: any): any {
    initWorker();
    Atomics.store(sharedBuf!, 0, 0);
    worker!.postMessage(req);
    Atomics.wait(sharedBuf!, 0, 0, 5000);
    const resp = responseBuffer;
    if (resp?.error) {
      throw new Error(resp.error);
    }
    return resp?.result;
  }

  function normalizeParams(args: any[]): any {
    if (args.length === 1 && Array.isArray(args[0])) {
      return args[0];
    }
    if (args.length === 1 && typeof args[0] === 'object' && args[0] !== null) {
      return args[0];
    }
    return args;
  }

  return {
    exec: (sql: string) => callSync({ type: 'EXEC', sql }),
    pragma: (_sql: string) => {
      try { callSync({ type: 'EXEC', sql: `PRAGMA ${_sql}` }); } catch (e) {}
    },
    prepare: (sql: string) => {
      return {
        get: (...args: any[]) => callSync({ type: 'PREPARE_GET', sql, params: normalizeParams(args) }),
        all: (...args: any[]) => callSync({ type: 'PREPARE_ALL', sql, params: normalizeParams(args) }),
        run: (...args: any[]) => callSync({ type: 'PREPARE_RUN', sql, params: normalizeParams(args) }),
      };
    },
    transaction: (fn: any) => {
      return (...args: any[]) => {
        callSync({ type: 'EXEC', sql: 'BEGIN TRANSACTION' });
        try {
          const result = fn(...args);
          callSync({ type: 'EXEC', sql: 'COMMIT' });
          return result;
        } catch (err) {
          callSync({ type: 'EXEC', sql: 'ROLLBACK' });
          throw err;
        }
      };
    },
    close: () => {
      if (worker) {
        try { callSync({ type: 'CLOSE' }); } catch (e) {}
        try { worker.terminate(); } catch (e) {}
        worker = null;
      }
    },
  };
}

export function getDb(): any {
  if (!dbInstance) {
    try {
      const dbDir = path.dirname(dbPath);
      if (!fs.existsSync(dbDir)) {
        fs.mkdirSync(dbDir, { recursive: true });
      }
      const Database = require('better-sqlite3');
      dbInstance = new Database(dbPath);
      dbInstance.pragma('foreign_keys = ON');
      if (!isVercel) {
        dbInstance.pragma('journal_mode = WAL');
      }
    } catch (err) {
      console.log('[Database] Native better-sqlite3 unavailable, using worker sql.js WASM engine.');
      dbInstance = createWorkerSqlJsDb(dbPath);
    }
  }
  return dbInstance;
}

export function closeDb(): void {
  if (dbInstance) {
    try {
      dbInstance.close();
    } catch {}
    dbInstance = null;
  }
}


