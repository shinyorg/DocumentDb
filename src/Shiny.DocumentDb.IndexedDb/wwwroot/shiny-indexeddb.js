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

export async function put(storeName, record) {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, 'readwrite');
        const store = tx.objectStore(storeName);
        const request = store.put(record);

        request.onsuccess = () => resolve();
        request.onerror = () => reject(new Error(`Put failed: ${request.error}`));
    });
}

export async function remove(storeName, key) {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, 'readwrite');
        const store = tx.objectStore(storeName);

        // Check if exists first
        const getReq = store.get(key);
        getReq.onsuccess = () => {
            if (!getReq.result) {
                resolve(false);
                return;
            }
            const delReq = store.delete(key);
            delReq.onsuccess = () => resolve(true);
            delReq.onerror = () => reject(new Error(`Delete failed: ${delReq.error}`));
        };
        getReq.onerror = () => reject(new Error(`Get failed: ${getReq.error}`));
    });
}

export async function getAllByTypeName(storeName, typeName) {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, 'readonly');
        const store = tx.objectStore(storeName);
        const index = store.index('typeName');
        const request = index.getAll(typeName);

        request.onsuccess = () => resolve(request.result || []);
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
        const request = index.getAllKeys(typeName);

        request.onsuccess = () => {
            const keys = request.result;
            let deleted = 0;
            if (keys.length === 0) {
                resolve(0);
                return;
            }
            for (const key of keys) {
                const delReq = store.delete(key);
                delReq.onsuccess = () => {
                    deleted++;
                    if (deleted === keys.length) resolve(deleted);
                };
                delReq.onerror = () => reject(new Error(`Delete failed: ${delReq.error}`));
            }
        };
        request.onerror = () => reject(new Error(`GetAllKeys failed: ${request.error}`));
    });
}

export async function batchPut(storeName, records) {
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
