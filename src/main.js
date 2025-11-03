import { Client, TablesDB, ID } from 'node-appwrite';

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
 * - COLLECTION_PURCHASES
 */

export default async function ({ req, res, log, error }) {
  const client = new Client()
    .setEndpoint(process.env.APPWRITE_ENDPOINT)
    .setProject(process.env.APPWRITE_PROJECT_ID)
    .setKey(process.env.APPWRITE_API_KEY);

  const databases = new TablesDB(client);

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
        return await handleBatchUpdateProducts(
          databases,
          data,
          log,
          error,
          res
        );
      case 'createGroupPurchaseWithSync':
        return await handleCreateGroupPurchaseWithSync(
          databases,
          data,
          log,
          error,
          res
        );
      default:
        return res.json({ error: 'Unknown operation' }, 400);
    }
  } catch (e) {
    error(e.message);
    return res.json({ error: e.message }, 500);
  }
}

/**
 * Crée des achats groupés avec synchronisation de produits en utilisant une transaction Appwrite
 * @param {TablesDB} databases - Instance Appwrite TablesDB
 * @param {Object} data - Données de l'opération groupée
 * @param {Function} log - Logger
 * @param {Function} error - Error logger
 * @returns {Object} Résultat de l'opération
 */
async function handleCreateGroupPurchaseWithSync(
  databases,
  data,
  log,
  error,
  res
) {
  const { mainId, batchData, invoiceData } = data;

  if (!mainId || !batchData || !invoiceData) {
    return res.json(
      {
        error: 'Missing required parameters: mainId, batchData, invoiceData',
      },
      400
    );
  }

  const { productsToCreate = [], purchasesToCreate = [] } = batchData;

  // Calculer le nombre total d'opérations
  const totalOperations = productsToCreate.length + purchasesToCreate.length;

  // Limiter le nombre d'opérations par transaction
  const maxOperations = 100; // Plan Free Appwrite
  if (totalOperations > maxOperations) {
    return res.json(
      {
        error: `Too many operations (${totalOperations}). Maximum ${maxOperations} operations per transaction`,
      },
      400
    );
  }

  if (!invoiceData.invoiceId) {
    return res.json(
      {
        error: 'invoiceId is required in invoiceData',
      },
      400
    );
  }

  log(
    `Starting group purchase with sync: ${productsToCreate.length} products to create, ${purchasesToCreate.length} purchases to create, total operations: ${totalOperations}`
  );

  // Debug: Log environment variables
  log(
    `Environment variables: DATABASE_ID=${process.env.DATABASE_ID}, COLLECTION_PRODUCTS=${process.env.COLLECTION_PRODUCTS}, COLLECTION_PURCHASES=${process.env.COLLECTION_PURCHASES}`
  );

  let transaction = null;

  try {
    // 1. Créer la transaction
    transaction = await databases.createTransaction();
    log(`Transaction created: ${transaction.$id}`);

    // 2. Préparer les opérations de création de produits
    const productOperations = productsToCreate.map((product) => ({
      action: 'create',
      databaseId: process.env.DATABASE_ID,
      tableId: process.env.COLLECTION_PRODUCTS,
      rowId: product.productId,
      data: {
        $id: product.productId, // Forcer l'ID local
        ...transformProductToAppwrite(product.productData),
      },
    }));

    // 3. Préparer les opérations de création d'achats
    const purchaseOperations = purchasesToCreate.map((purchase) => ({
      action: 'create',
      databaseId: process.env.DATABASE_ID,
      tableId: process.env.COLLECTION_PURCHASES,
      rowId: ID.unique(), // Générer un nouvel ID pour chaque achat
      data: {
        products: [purchase.productId],
        mainId: mainId,
        quantity: purchase.quantity,
        unit: purchase.unit,
        status: purchase.status || 'delivered',
        notes: purchase.notes || '',
        store: invoiceData.store || null,
        who: purchase.who || null,
        price: null,
        invoiceId: invoiceData.invoiceId,
        invoiceTotal: null,
        orderDate: purchase.orderDate || null,
        deliveryDate: purchase.deliveryDate || null,
        createdBy: purchase.createdBy || null,
      },
    }));

    // 4. Combiner toutes les opérations
    const allOperations = [...productOperations, ...purchaseOperations];
    log(
      `Preparing ${productOperations.length} product creations and ${purchaseOperations.length} purchase creations`
    );

    // 5. Stager les opérations
    await databases.createOperations({
      databaseId: process.env.DATABASE_ID,
      transactionId: transaction.$id,
      operations: allOperations,
    });

    log(`Staged ${allOperations.length} operations`);

    // 6. Commit la transaction
    const result = await databases.updateTransaction({
      transactionId: transaction.$id,
      commit: true,
    });

    log(`Transaction committed successfully`);

    // 7. Créer une dépense globale si invoiceTotal est fourni
    let expenseResult = null;
    if (invoiceData.invoiceTotal) {
      try {
        // Créer une dépense globale dans une transaction séparée
        const expenseTransaction = await databases.createTransaction();

        const expenseOperation = {
          action: 'create',
          databaseId: process.env.DATABASE_ID,
          tableId: process.env.COLLECTION_PURCHASES,
          rowId: ID.unique(),
          data: {
            products: [], // Pas de produits liés pour une dépense globale
            mainId: mainId,
            quantity: 1,
            unit: 'global',
            status: 'expense',
            notes:
              invoiceData.notes ||
              `Facture globale pour ${purchasesToCreate.length} produits`,
            store: invoiceData.store || null,
            who: invoiceData.who || null,
            price: null,
            invoiceId: invoiceData.invoiceId,
            invoiceTotal: invoiceData.invoiceTotal,
            orderDate: null,
            deliveryDate: null,
            createdBy: null,
          },
        };

        await databases.createOperations({
          databaseId: process.env.DATABASE_ID,
          transactionId: expenseTransaction.$id,
          operations: [expenseOperation],
        });

        const expenseTransactionResult = await databases.updateTransaction({
          transactionId: expenseTransaction.$id,
          commit: true,
        });

        log(`Expense transaction committed successfully`);
        expenseResult = expenseTransactionResult;
      } catch (expenseError) {
        log(
          `Warning: Failed to create global expense: ${expenseError.message}`
        );
        // On ne fait pas échouer toute l'opération si juste la dépense globale échoue
      }
    }

    return res.json({
      success: true,
      transactionId: transaction.$id,
      productsCreated: productsToCreate.length,
      purchasesCreated: purchasesToCreate.length,
      expenseCreated: !!expenseResult,
      totalOperations,
      invoiceId: invoiceData.invoiceId,
      timestamp: new Date().toISOString(),
    });
  } catch (transactionError) {
    error(`Transaction failed: ${transactionError.message}`);

    // Tenter de rollback si possible
    if (transaction) {
      try {
        // Petit délai pour laisser la transaction se stabiliser
        await new Promise((resolve) => setTimeout(resolve, 100));

        await databases.updateTransaction({
          transactionId: transaction.$id,
          rollback: true,
        });
        log('Transaction rolled back');
      } catch (rollbackError) {
        if (rollbackError.message.includes('not ready yet')) {
          log('Transaction not ready for rollback - will expire automatically');
        } else {
          error(`Rollback failed: ${rollbackError.message}`);
        }
      }
    } else {
      log('No transaction to rollback - creation failed');
    }

    return res.json(
      {
        success: false,
        error: transactionError.message,
        totalOperations,
      },
      500
    );
  }
}

/**
 * Transforme un EnrichedProduct en données Appwrite (similaire à enrichedProductToAppwriteProduct côté client)
 * @param {Object} product - Produit enrichi
 * @param {Object} batchUpdates - Updates batch à appliquer
 * @returns {Object} Données formatées pour Appwrite
 */
function transformProductToAppwrite(product, batchUpdates = {}) {
  // Uniquement les champs métier, exclure toutes les métadonnées système ($*)
  const baseData = {
    productHugoUuid: product.productHugoUuid,
    productName: product.productName,
    mainId: product.mainId,
    status: product.status || null,
    who: product.who || null,
    store: product.store || null,
    stockReel: product.stockReel || null,
    previousNames: product.previousNames || null,
    isMerged: product.isMerged || false,
    mergedFrom: product.mergedFrom || null,
    mergeDate: product.mergeDate || null,
    mergeReason: product.mergeReason || null,
    mergedInto: product.mergedInto || null,
    totalNeededOverride: product.totalNeededOverride || null,
  };

  // Appliquer les updates batch par-dessus les données de base
  return {
    ...baseData,
    ...batchUpdates,
  };
}

/**
 * Applique les updates batch en fonction du type
 * @param {string} updateType - Type de mise à jour
 * @param {*} updateData - Données de mise à jour
 * @param {Object} options - Options supplémentaires
 * @returns {Object} Updates à appliquer
 */
function prepareBatchUpdates(updateType, updateData, options = {}) {
  switch (updateType) {
    case 'store':
      // updateData: { storeName: string, storeComment?: string }
      return {
        store: JSON.stringify(updateData),
      };

    case 'who':
      // updateData: { names: string[] }
      if (options.mode === 'add') {
        // Mode 'add' : logique gérée côté client en fusionnant les listes
        return {
          who: updateData.names,
        };
      } else {
        // Mode 'replace'
        return {
          who: updateData.names,
        };
      }

    default:
      throw new Error(`Unsupported update type: ${updateType}`);
  }
}

/**
 * Met à jour plusieurs produits en utilisant une transaction Appwrite
 * @param {TablesDB} databases - Instance Appwrite TablesDB
 * @param {Object} data - Données de la mise à jour groupée
 * @param {Function} log - Logger
 * @param {Function} error - Error logger
 * @returns {Object} Résultat de l'opération
 */
async function handleBatchUpdateProducts(databases, data, log, error, res) {
  const { productIds, products, updateType, updateData, options = {} } = data;

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

  let transaction = null;

  try {
    // 1. Créer la transaction (sans paramètres dans le SDK 20.0.0)
    transaction = await databases.createTransaction();

    log(`Transaction created: ${transaction.$id}`);

    // 2. Préparer les opérations mixtes (créations + mises à jour)
    const operations = productIds.map((productId) => {
      // Récupérer le produit complet depuis les données envoyées
      const product = products.find((p) => p.$id === productId);
      if (!product) {
        throw new Error(`Product ${productId} not found in products data`);
      }

      // Préparer les updates batch
      const batchUpdates = prepareBatchUpdates(updateType, updateData, options);

      // Transformer le produit en données Appwrite avec les updates batch
      const appwriteData = transformProductToAppwrite(product, batchUpdates);

      // Déterminer si c'est une création ou une mise à jour
      const isSynced = product.isSynced === true;

      return {
        action: isSynced ? 'update' : 'create',
        databaseId: process.env.DATABASE_ID,
        tableId: process.env.COLLECTION_PRODUCTS,
        rowId: productId,
        data: {
          $id: productId, // Forcer l'ID local pour les créations
          ...appwriteData,
        },
      };
    });

    // Logger les statistiques
    const createCount = operations.filter(
      (op) => op.action === 'create'
    ).length;
    const updateCount = operations.filter(
      (op) => op.action === 'update'
    ).length;
    log(`Preparing ${createCount} creations and ${updateCount} updates`);

    // 3. Stager les opérations avec la bonne syntaxe SDK 20.0.0
    await databases.createOperations({
      databaseId: process.env.DATABASE_ID,
      transactionId: transaction.$id,
      operations: operations,
    });

    log(`Staged ${operations.length} operations`);

    // 4. Commit la transaction
    const result = await databases.updateTransaction({
      transactionId: transaction.$id,
      commit: true,
    });

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

    // Tenter de rollback si possible (avec délai pour éviter le "not ready yet")
    if (transaction) {
      try {
        // Petit délai pour laisser la transaction se stabiliser
        await new Promise((resolve) => setTimeout(resolve, 100));

        await databases.updateTransaction({
          transactionId: transaction.$id,
          rollback: true,
        });
        log('Transaction rolled back');
      } catch (rollbackError) {
        if (rollbackError.message.includes('not ready yet')) {
          log('Transaction not ready for rollback - will expire automatically');
        } else {
          error(`Rollback failed: ${rollbackError.message}`);
        }
      }
    } else {
      log('No transaction to rollback - creation failed');
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
