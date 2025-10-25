import { Client, TablesDB, Users } from 'node-appwrite';

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

export default async function createProductsList({ req, res, log, error: logError }) {
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

    // 3. Vérification que l'événement n'existe pas déjà
    log(`[Appwrite Function] Vérification de l'existence de ${eventId}...`);
    try {
      await tablesDB.getRow({
        databaseId: process.env.DATABASE_ID,
        tableId: process.env.COLLECTION_MAIN,
        rowId: eventId
      });
      log(
        `[Appwrite Function] L'événement ${eventId} existe déjà dans main`
      );
      return res.json(
        { error: 'Cet événement existe déjà', code: 'already_exists' },
        409
      );
    } catch (err) {
      if (err.code !== 404) {
        throw err;
      }
      // 404 = document n'existe pas, c'est ce qu'on veut
    }

    // 4. Créer une transaction
    log(`[Appwrite Function] Création de la transaction...`);
    
    const transaction = await tablesDB.createTransaction({
      ttl: 120 // 2 minutes pour laisser le temps aux opérations
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
      transactionId: transactionId
    });

    log(`[Appwrite Function] Document main créé dans la transaction`);

    // 6. Créer tous les produits en bulk dans la transaction
    if (eventData.ingredients && Array.isArray(eventData.ingredients)) {
      log(`[Appwrite Function] Préparation de ${eventData.ingredients.length} produits...`);
      
      const productsData = eventData.ingredients.map((ingredient) => ({
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

      // Utiliser createRows avec transactionId
      log(`[Appwrite Function] Création bulk de ${productsData.length} produits...`);
      await tablesDB.createRows({
        databaseId: process.env.DATABASE_ID,
        tableId: process.env.COLLECTION_PRODUCTS,
        rows: productsData,
        permissions: undefined,
        transactionId: transactionId
      });

      log(
        `[Appwrite Function] ${productsData.length} produits créés dans la transaction`
      );
    }

    // 7. Valider (commit) la transaction
    await tablesDB.updateTransaction({
      transactionId: transactionId,
      commit: true
    });

    log(`[Appwrite Function] Transaction validée avec succès`);

    return res.json({
      success: true,
      eventId,
      transactionId,
      message: 'Liste de produits créée avec succès (transaction validée)',
    });
    
  } catch (err) {
    logError(
      `[Appwrite Function] Erreur lors de la création: ${err.message}`
    );
    logError(`[Appwrite Function] Stack: ${err.stack}`);

    // En cas d'erreur, annuler la transaction si elle existe
    if (transactionId) {
      try {
        await tablesDB.updateTransaction({
          transactionId: transactionId,
          rollback: true
        });
        log(`[Appwrite Function] Transaction annulée (rollback)`);
      } catch (rollbackError) {
        logError(
          `[Appwrite Function] Erreur lors du rollback: ${rollbackError.message}`
        );
      }
    }

    // Gestion des erreurs spécifiques
    if (err.code === 409 || err.code === 'document_already_exists') {
      return res.json(
        {
          error:
            'Conflit détecté: un ou plusieurs documents existent déjà',
          code: 'conflict',
        },
        409
      );
    }

    return res.json(
      { 
        error: err.message || 'Erreur interne du serveur', 
        code: err.code,
        rolledBack: !!transactionId 
      },
      500
    );
  }
}
