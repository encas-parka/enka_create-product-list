import { Client, TablesDB } from 'node-appwrite';

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

export default async ({ req, res, log, error }) => {
  let transactionId = null;

  try {
    // 1. Parse le body JSON
    const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;

    // 2. Validation des données d'entrée
    const { eventId, eventData, contentHash, userId } = body;

    if (!eventId || !eventData || !contentHash || !userId) {
      return res.json(
        {
          error:
            'Données manquantes: eventId, eventData, contentHash, userId requis',
        },
        400
      );
    }

    log(
      `[Appwrite Function] Début de création pour l'événement ${eventId} par ${userId}`
    );

    // 2. Initialisation du client Appwrite côté serveur
    const client = new Client()
      .setEndpoint(process.env.APPWRITE_ENDPOINT)
      .setProject(process.env.APPWRITE_PROJECT_ID)
      .setKey(process.env.APPWRITE_API_KEY);

    const tablesDB = new TablesDB(client);

    // 4. Créer une transaction
    log(`[Appwrite Function] Création de la transaction...`);

    const transaction = await tablesDB.createTransaction({
      ttl: 120, // 2 minutes pour laisser le temps aux opérations
    });

    transactionId = transaction.$id;
    log(`[Appwrite Function] Transaction créée: ${transactionId}`);

    // 5. Créer le document main dans la transaction
    log(`[Appwrite Function] Création du document main...`);
    await tablesDB.createRow({
      databaseId: process.env.DATABASE_ID,
      tableId: process.env.COLLECTION_MAIN,
      rowId: eventId,
      data: {
        name: eventData.name || `Événement ${eventId}`,
        originalDataHash: contentHash,
        isActive: true,
        createdBy: userId,
        status: 'active',
        error: null,
        allDates: JSON.stringify(eventData.allDates || []),
      },
      permissions: undefined,
      transactionId: transactionId,
    });

    log(`[Appwrite Function] Document main créé dans la transaction`);

    // 6. Créer tous les produits en bulk dans la transaction (par lots de 100 pour le free tier)
    if (eventData.ingredients && Array.isArray(eventData.ingredients)) {
      const ingredients = eventData.ingredients;
      const batchSize = 100;
      const totalBatches = Math.ceil(ingredients.length / batchSize);

      log(
        `[Appwrite Function] Préparation de ${ingredients.length} produits en ${totalBatches} lot(s) de ${batchSize}...`
      );

      for (let i = 0; i < ingredients.length; i += batchSize) {
        const batch = ingredients.slice(i, i + batchSize);
        const batchNumber = Math.floor(i / batchSize) + 1;

        const productsData = batch.map((ingredient) => ({
          $id: `${ingredient.ingredientHugoUuid}_${eventId}`,
          productHugoUuid: ingredient.ingredientHugoUuid || '',
          productName: ingredient.ingredientName || '',
          productType: ingredient.ingType || '',
          mainId: eventId,
          totalNeededConsolidated: JSON.stringify(
            ingredient.totalNeededConsolidated || []
          ),
          totalNeededRaw: JSON.stringify(ingredient.totalNeededRaw || []),
          neededConsolidatedByDate: JSON.stringify(
            ingredient.neededConsolidatedByDate || []
          ),
          recipesOccurrences: JSON.stringify(
            ingredient.recipesOccurrences || []
          ),
          pFrais: ingredient.pFrais || false,
          pSurgel: ingredient.pSurgel || false,
          nbRecipes: ingredient.nbRecipes || 0,
          totalAssiettes: ingredient.totalAssiettes || 0,
          conversionRules: ingredient.conversionRules || null,
        }));

        // Créer les produits individuellement dans la transaction
        log(
          `[Appwrite Function] Création du lot ${batchNumber}/${totalBatches} (${productsData.length} produits)...`
        );

        for (const productData of productsData) {
          await tablesDB.createRow({
            databaseId: process.env.DATABASE_ID,
            tableId: process.env.COLLECTION_PRODUCTS,
            rowId: productData.$id,
            data: productData,
            permissions: undefined,
            transactionId: transactionId,
          });
        }

        log(
          `[Appwrite Function] Lot ${batchNumber}/${totalBatches} créé avec succès`
        );
      }

      log(
        `[Appwrite Function] ${ingredients.length} produits créés dans la transaction (${totalBatches} lot(s))`
      );
    }

    // 7. Valider (commit) la transaction
    await tablesDB.updateTransaction({
      transactionId: transactionId,
      commit: true,
    });

    log(`[Appwrite Function] Transaction validée avec succès`);

    return res.json({
      success: true,
      eventId,
      transactionId,
      message: 'Liste de produits créée avec succès (transaction validée)',
    });
  } catch (err) {
    error(`[Appwrite Function] Erreur lors de la création: ${err.message}`);
    error(`[Appwrite Function] Stack: ${err.stack}`);

    // En cas d'erreur, annuler la transaction si elle existe
    if (transactionId) {
      try {
        await TablesDB.updateTransaction({
          transactionId: transactionId,
          rollback: true,
        });
        log(`[Appwrite Function] Transaction annulée (rollback)`);
      } catch (rollbackError) {
        error(
          `[Appwrite Function] Erreur lors du rollback: ${rollbackError.message}`
        );
      }
    }

    // Gestion des erreurs spécifiques
    if (err.code === 409 || err.code === 'document_already_exists') {
      return res.json(
        {
          error: 'Conflit détecté: un ou plusieurs documents existent déjà',
          code: 'conflict',
        },
        409
      );
    }

    return res.json(
      {
        error: err.message || 'Erreur interne du serveur',
        code: err.code,
        rolledBack: !!transactionId,
      },
      500
    );
  }
};
