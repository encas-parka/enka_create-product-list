import { Client, Tables, ID } from 'node-appwrite';

/**
 * Fonction Appwrite pour créer une liste de produits transactionnelle
 * Utilise l'API Transactions pour garantir l'atomicité complète
 *
 * Variables d'environnement requises:
 * - APPWRITE_API_KEY (clé API avec permissions admin)
 * - APPWRITE_ENDPOINT
 * - APPWRITE_PROJECT_ID
 * - DATABASE_ID
 * - COLLECTION_MAIN
 * - COLLECTION_PRODUCTS
 */

export default async function ({ req, res, log, error }) {
  const client = new Client()
    .setEndpoint(process.env.APPWRITE_ENDPOINT)
    .setProject(process.env.APPWRITE_PROJECT_ID)
    .setKey(process.env.APPWRITE_API_KEY);

  const databases = new Tables(client);

  // Parser le corps de la requête
  let payload;
  try {
    payload = JSON.parse(req.body);
  } catch (e) {
    return res.json({ error: 'Invalid JSON payload' }, 400);
  }

  const { operation, data } = payload;

  try {
    switch (operation) {
      case 'batchUpdateProducts':
        return await handleBatchUpdateProducts(databases, data, log, error);
      default:
        return res.json({ error: 'Unknown operation' }, 400);
    }
  } catch (e) {
    error(e.message);
    return res.json({ error: e.message }, 500);
  }
}

/**
 * Met à jour plusieurs produits en utilisant une transaction Appwrite
 * @param {Tables} databases - Instance Appwrite Tables
 * @param {Object} data - Données de la mise à jour groupée
 * @param {Function} log - Logger
 * @param {Function} error - Error logger
 * @returns {Object} Résultat de l'opération
 */
async function handleBatchUpdateProducts(databases, data, log, error) {
  const { productIds, updateType, updateData, options = {} } = data;

  if (!productIds?.length || !updateType || !updateData) {
    return res.json(
      {
        error:
          'Missing required parameters: productIds, updateType, updateData',
      },
      400
    );
  }

  // Limiter le nombre d'opérations par transaction
  const maxOperations = 100; // Plan Free Appwrite
  if (productIds.length > maxOperations) {
    return res.json(
      {
        error: `Too many products. Maximum ${maxOperations} operations per transaction`,
      },
      400
    );
  }

  log(
    `Starting batch update for ${productIds.length} products, type: ${updateType}`
  );

  try {
    // 1. Créer la transaction
    const transaction = await databases.createTransaction(
      process.env.DATABASE_ID
    );

    log(`Transaction created: ${transaction.$id}`);

    // 2. Préparer les opérations en fonction du type de mise à jour
    const operations = productIds.map((productId) => {
      const updatePayload = prepareUpdatePayload(
        updateType,
        updateData,
        options
      );

      return {
        action: 'update',
        databaseId: process.env.DATABASE_ID,
        collectionId: process.env.COLLECTION_PRODUCTS,
        documentId: productId,
        data: updatePayload,
      };
    });

    // 3. Stager les opérations
    await databases.createOperations(
      process.env.DATABASE_ID,
      transaction.$id,
      operations
    );

    log(`Staged ${operations.length} operations`);

    // 4. Commit la transaction
    const result = await databases.updateTransaction(
      process.env.DATABASE_ID,
      transaction.$id,
      'commit'
    );

    log(`Transaction committed successfully`);

    return res.json({
      success: true,
      transactionId: transaction.$id,
      updatedCount: productIds.length,
      updateType,
      timestamp: new Date().toISOString(),
    });
  } catch (transactionError) {
    error(`Transaction failed: ${transactionError.message}`);

    // Tenter de rollback si possible
    try {
      await databases.updateTransaction(
        process.env.DATABASE_ID,
        transaction.$id,
        'rollback'
      );
      log('Transaction rolled back');
    } catch (rollbackError) {
      error(`Rollback failed: ${rollbackError.message}`);
    }

    return res.json(
      {
        success: false,
        error: transactionError.message,
        productIds: productIds.length,
      },
      500
    );
  }
}

/**
 * Prépare le payload de mise à jour en fonction du type
 * @param {string} updateType - Type de mise à jour (store, who, etc.)
 * @param {*} updateData - Données de mise à jour
 * @param {Object} options - Options supplémentaires
 * @returns {Object} Payload pour Appwrite
 */
function prepareUpdatePayload(updateType, updateData, options) {
  switch (updateType) {
    case 'store':
      // updateData: { storeName: string, storeComment?: string }
      return {
        store: JSON.stringify(updateData),
      };

    case 'who':
      // updateData: { names: string[], mode: 'replace'|'add' }
      if (options.mode === 'add') {
        // Pour le mode 'add', on doit récupérer les valeurs existantes
        // et ajouter les nouvelles (logique gérée côté client)
        return {
          who: updateData.names,
        };
      } else {
        // Mode 'replace'
        return {
          who: updateData.names,
        };
      }

    case 'stock':
      // updateData: { quantity: number, unit: string, notes?: string }
      return {
        stockReel: JSON.stringify([
          {
            quantity: updateData.quantity.toString(),
            unit: updateData.unit,
            notes: updateData.notes || '',
            dateTime: new Date().toISOString(),
          },
        ]),
      };

    default:
      throw new Error(`Unsupported update type: ${updateType}`);
  }
}
