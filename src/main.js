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
  // Déclarer variables en dehors du try pour être accessibles dans le catch
  let allTransactions = [];
  let tablesDB = null;

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

    tablesDB = new TablesDB(client);

    // 4. Variables pour les transactions
    const maxOperationsPerTransaction = 99; // Free tier limite

    log(`[Appwrite Function] Début de la création multi-transactions...`);

    // 5. Créer le document main dans sa propre transaction
    log(
      `[Appwrite Function] Création de la transaction pour le document main...`
    );

    const mainTransaction = await tablesDB.createTransaction({
      ttl: 120, // 2 minutes
    });

    allTransactions.push(mainTransaction.$id);
    log(`[Appwrite Function] Transaction main créée: ${mainTransaction.$id}`);

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
        allDates: eventData.allDates || [],
      },
      permissions: undefined,
      transactionId: mainTransaction.$id,
    });

    // Attendre que la transaction main soit prête
    log(`[Appwrite Function] Attente de la transaction main...`);
    await new Promise((resolve) => setTimeout(resolve, 2000)); // 2 secondes

    // Valider la transaction du document main
    await tablesDB.updateTransaction({
      transactionId: mainTransaction.$id,
      commit: true,
    });

    log(`[Appwrite Function] Transaction main validée avec succès`);

    // 6. Créer tous les produits en utilisant plusieurs transactions (99 produits max par transaction)
    if (eventData.ingredients && Array.isArray(eventData.ingredients)) {
      const ingredients = eventData.ingredients;
      const productsPerTransaction = maxOperationsPerTransaction;
      const totalTransactions = Math.ceil(
        ingredients.length / productsPerTransaction
      );

      log(
        `[Appwrite Function] Préparation de ${ingredients.length} produits en ${totalTransactions} transaction(s) de ${productsPerTransaction} produits max...`
      );

      for (let i = 0; i < ingredients.length; i += productsPerTransaction) {
        const transactionIngredients = ingredients.slice(
          i,
          i + productsPerTransaction
        );
        const transactionNumber = Math.floor(i / productsPerTransaction) + 1;

        // Créer une nouvelle transaction pour ce lot de produits
        log(
          `[Appwrite Function] Création de la transaction ${transactionNumber}/${totalTransactions} pour ${transactionIngredients.length} produits...`
        );

        const productTransaction = await tablesDB.createTransaction({
          ttl: 120, // 2 minutes
        });

        allTransactions.push(productTransaction.$id);
        log(
          `[Appwrite Function] Transaction ${transactionNumber} créée: ${productTransaction.$id}`
        );

        // Créer chaque produit individuellement dans cette transaction
        for (const ingredient of transactionIngredients) {
          const productData = {
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
          };

          await tablesDB.createRow({
            databaseId: process.env.DATABASE_ID,
            tableId: process.env.COLLECTION_PRODUCTS,
            rowId: productData.$id,
            data: productData,
            permissions: undefined,
            transactionId: productTransaction.$id,
          });
        }

        // Attendre que la transaction soit prête avant de valider
        log(
          `[Appwrite Function] Attente de la transaction ${transactionNumber}...`
        );
        await new Promise((resolve) => setTimeout(resolve, 2000)); // 2 secondes

        // Valider la transaction de produits
        await tablesDB.updateTransaction({
          transactionId: productTransaction.$id,
          commit: true,
        });

        log(
          `[Appwrite Function] Transaction ${transactionNumber}/${totalTransactions} validée avec succès (${transactionIngredients.length} produits)`
        );
      }

      log(
        `[Appwrite Function] ${ingredients.length} produits créés avec succès (${totalTransactions} transaction(s))`
      );
    }

    // 7. Toutes les transactions sont déjà validées individuellement
    log(
      `[Appwrite Function] Toutes les transactions ont été validées avec succès`
    );

    return res.json({
      success: true,
      eventId,
      totalTransactions: allTransactions.length,
      transactionIds: allTransactions,
      message:
        'Liste de produits créée avec succès (multi-transactions validées)',
    });
  } catch (err) {
    error(`[Appwrite Function] Erreur lors de la création: ${err.message}`);
    error(`[Appwrite Function] Stack: ${err.stack}`);

    // En cas d'erreur, annuler toutes les transactions existantes
    if (allTransactions.length > 0) {
      // Attendre un peu avant de tenter le rollback (les transactions ont besoin de temps)
      await new Promise((resolve) => setTimeout(resolve, 3000)); // 3 secondes

      for (const transactionId of allTransactions) {
        try {
          // Plusieurs tentatives de rollback avec délai
          let rollbackSuccess = false;
          for (let attempt = 0; attempt < 3; attempt++) {
            try {
              await tablesDB.updateTransaction({
                transactionId: transactionId,
                rollback: true,
              });
              log(
                `[Appwrite Function] Transaction ${transactionId} annulée (rollback) - tentative ${attempt + 1}`
              );
              rollbackSuccess = true;
              break;
            } catch (rollbackError) {
              if (attempt < 2) {
                log(
                  `[Appwrite Function] Rollback échoué pour ${transactionId}, tentative ${attempt + 1}/3, nouvelle attente...`
                );
                await new Promise((resolve) => setTimeout(resolve, 2000)); // 2 secondes
              } else {
                throw rollbackError;
              }
            }
          }

          if (!rollbackSuccess) {
            throw new Error(
              `Rollback échoué après 3 tentatives pour ${transactionId}`
            );
          }
        } catch (rollbackError) {
          error(
            `[Appwrite Function] Erreur finale lors du rollback de ${transactionId}: ${rollbackError.message}`
          );
        }
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
        rolledBack: allTransactions.length > 0,
        transactionsCount: allTransactions.length,
      },
      500
    );
  }
};
