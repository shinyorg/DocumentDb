let db = null;

export async function initialize(databaseName, version, storeNames) {
    if (db) {
        db.close();
        db = null;
    }

    return new Promise((resolve, reject) => {
        const request = indexedDB.open(databaseName, version);

        request.onupgradeneeded = (event) => {
            const database = event.target.result;
            for (const storeName of storeNames) {
                if (!database.objectStoreNames.contains(storeName)) {
                    const store = database.createObjectStore(storeName, { keyPath: 'key' });
                    store.createIndex('typeName', 'typeName', { unique: false });
                }
            }
        };

        request.onsuccess = (event) => {
            db = event.target.result;
            resolve();
        };

        request.onerror = (event) => {
            reject(new Error(`Failed to open IndexedDB: ${event.target.error}`));
        };
    });
}

export async function get(storeName, key) {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, 'readonly');
        const store = tx.objectStore(storeName);
        const request = store.get(key);

        request.onsuccess = () => {
            resolve(request.result ? JSON.stringify(request.result) : null);
        };
        request.onerror = () => reject(new Error(`Get failed: ${request.error}`));
    });
}

// Called from [JSImport] — record arrives as a JSON string and is parsed here
// because [JSImport] cannot marshal arbitrary objects.
export async function put(storeName, recordJson) {
    const record = JSON.parse(recordJson);
    return new Promise((resolve, reject) => {
        // Resolve on tx.oncomplete (commit), not request.onsuccess — IndexedDB reports commit-time failures
        // (QuotaExceededError, constraint/abort) after the request succeeds, so resolving on the request would
        // be a false-positive success with the write silently rolled back.
        const tx = db.transaction(storeName, 'readwrite');
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(new Error(`Put failed: ${tx.error}`));
        tx.onabort = () => reject(new Error(`Put aborted: ${tx.error}`));
        tx.objectStore(storeName).put(record);
    });
}

export async function remove(storeName, key) {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, 'readwrite');
        const store = tx.objectStore(storeName);
        let existed = false;
        const getReq = store.get(key);
        getReq.onsuccess = () => {
            if (getReq.result) {
                existed = true;
                store.delete(key);
            }
        };
        // Report the outcome only once the transaction commits (so an abort surfaces as a failure).
        tx.oncomplete = () => resolve(existed);
        tx.onerror = () => reject(new Error(`Delete failed: ${tx.error}`));
        tx.onabort = () => reject(new Error(`Delete aborted: ${tx.error}`));
    });
}

// Returns a JSON string of the records array — [JSImport] cannot marshal
// arbitrary object arrays back to C# without serialization.
export async function getAllByTypeName(storeName, typeName) {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, 'readonly');
        const store = tx.objectStore(storeName);
        const index = store.index('typeName');
        const request = index.getAll(typeName);

        request.onsuccess = () => resolve(JSON.stringify(request.result || []));
        request.onerror = () => reject(new Error(`GetAll failed: ${request.error}`));
    });
}

export async function countByTypeName(storeName, typeName) {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, 'readonly');
        const store = tx.objectStore(storeName);
        const index = store.index('typeName');
        const request = index.count(typeName);

        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(new Error(`Count failed: ${request.error}`));
    });
}

export async function clearByTypeName(storeName, typeName) {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, 'readwrite');
        const store = tx.objectStore(storeName);
        const index = store.index('typeName');
        let deleted = 0;
        const request = index.getAllKeys(typeName);
        request.onsuccess = () => {
            for (const key of request.result) {
                store.delete(key);
                deleted++;
            }
        };
        // Resolve the count only once the whole batch commits.
        tx.oncomplete = () => resolve(deleted);
        tx.onerror = () => reject(new Error(`Clear failed: ${tx.error}`));
        tx.onabort = () => reject(new Error(`Clear aborted: ${tx.error}`));
    });
}

// Called from [JSImport] — records arrive as a JSON string and are parsed here.
export async function batchPut(storeName, recordsJson) {
    const records = JSON.parse(recordsJson);
    return new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, 'readwrite');
        const store = tx.objectStore(storeName);

        for (const record of records) {
            store.put(record);
        }

        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(new Error(`Batch put failed: ${tx.error}`));
    });
}

export async function batchDelete(storeName, keys) {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, 'readwrite');
        const store = tx.objectStore(storeName);

        for (const key of keys) {
            store.delete(key);
        }

        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(new Error(`Batch delete failed: ${tx.error}`));
    });
}
