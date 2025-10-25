import { Client, Databases } from 'node-appwrite';

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

export default async function createProductsList(req, res) {
  let transactionId = null;
  
  try {
    // 1. Validation des données d'entrée
    const { eventId, eventData, contentHash, userId } = req.body;

    if (!eventId || !eventData || !contentHash || !userId) {
      return res.json(
        {
          error:
            'Données manquantes: eventId, eventData, contentHash, userId requis',
        },
        400
      );
    }

    console.log(
      `[Appwrite Function] Début de création pour l'événement ${eventId} par ${userId}`
    );

    // 2. Initialisation du client Appwrite côté serveur
    const client = new Client()
      .setEndpoint(process.env.APPWRITE_ENDPOINT)
      .setProject(process.env.APPWRITE_PROJECT_ID)
      .setKey(process.env.APPWRITE_API_KEY);

    const databases = new Databases(client);

    // 3. Vérification que l'événement n'existe pas déjà
    try {
      await databases.getDocument(
        process.env.DATABASE_ID,
        process.env.COLLECTION_MAIN,
        eventId
      );
      console.log(
        `[Appwrite Function] L'événement ${eventId} existe déjà dans main`
      );
      return res.json(
        { error: 'Cet événement existe déjà', code: 'already_exists' },
        409
      );
    } catch (error) {
      if (error.code !== 404) {
        throw error;
      }
      // 404 = document n'existe pas, c'est ce qu'on veut
    }

    // 4. Créer une transaction (TTL par défaut: 60 secondes)
    const transaction = await databases.createTransaction({
      ttl: 120 // 2 minutes pour laisser le temps aux opérations
    });
    
    transactionId = transaction.$id;
    console.log(`[Appwrite Function] Transaction créée: ${transactionId}`);

    // 5. Créer le document main dans la transaction
    await databases.createDocument(
      process.env.DATABASE_ID,
      process.env.COLLECTION_MAIN,
      eventId,
      {
        name: eventData.name || `Événement ${eventId}`,
        originalDataHash: contentHash,
        isActive: true,
        createdBy: userId,
        status: 'active',
        error: null,
        allDates: eventData.allDates || [],
      },
      undefined, // permissions
      transactionId // Lier à la transaction
    );

    console.log(`[Appwrite Function] Document main créé dans la transaction`);

    // 6. Créer tous les produits en bulk dans la transaction
    if (eventData.ingredients && Array.isArray(eventData.ingredients)) {
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
      await databases.createRows(
        process.env.DATABASE_ID,
        process.env.COLLECTION_PRODUCTS,
        productsData,
        undefined, // permissions
        transactionId // Lier à la transaction
      );

      console.log(
        `[Appwrite Function] ${productsData.length} produits créés dans la transaction`
      );
    }

    // 7. Valider (commit) la transaction
    await databases.updateTransaction(
      transactionId,
      'commit' // ou 'rollback' pour annuler
    );

    console.log(`[Appwrite Function] Transaction validée avec succès`);

    return res.json({
      success: true,
      eventId,
      transactionId,
      message: 'Liste de produits créée avec succès (transaction validée)',
    });
    
  } catch (error) {
    console.error(
      `[Appwrite Function] Erreur lors de la création:`,
      error.message
    );

    // En cas d'erreur, annuler la transaction si elle existe
    if (transactionId) {
      try {
        await databases.updateTransaction(transactionId, 'rollback');
        console.log(`[Appwrite Function] Transaction annulée (rollback)`);
      } catch (rollbackError) {
        console.error(
          `[Appwrite Function] Erreur lors du rollback:`,
          rollbackError.message
        );
      }
    }

    // Gestion des erreurs spécifiques
    if (error.code === 409 || error.code === 'document_already_exists') {
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
        error: error.message || 'Erreur interne du serveur', 
        code: error.code,
        rolledBack: !!transactionId 
      },
      500
    );
  }
}
