import { Client, Databases } from "node-appwrite";

/**
 * Fonction Appwrite pour créer une liste de produits transactionnelle
 * Déclenche une transaction côté serveur avec permissions admin
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
  try {
    console.log("[Appwrite Function] Début de l'exécution");
    console.log("[Appwrite Function] req.payload:", req.payload);
    console.log("[Appwrite Function] req.body:", req.body);

    // 1. Parser les données d'entrée (Appwrite envoie en req.payload ou req.body)
    let inputData;
    
    if (req.payload) {
      // Cas 1: Les données viennent en tant que string dans payload
      if (typeof req.payload === "string") {
        inputData = JSON.parse(req.payload);
      } else {
        inputData = req.payload;
      }
    } else if (req.body) {
      // Cas 2: Les données viennent en tant que string dans body
      if (typeof req.body === "string") {
        inputData = JSON.parse(req.body);
      } else {
        inputData = req.body;
      }
    } else {
      throw new Error("Aucune donnée reçue dans la requête");
    }

    const { eventId, eventData, contentHash, userId } = inputData;

    console.log("[Appwrite Function] Données parsées:", {
      eventId,
      userId,
      hasEventData: !!eventData,
      contentHash,
    });

    // 2. Validation des données d'entrée
    if (!eventId || !eventData || !contentHash || !userId) {
      console.error("[Appwrite Function] Données manquantes", {
        eventId: !!eventId,
        eventData: !!eventData,
        contentHash: !!contentHash,
        userId: !!userId,
      });
      return res.json(
        {
          error:
            "Données manquantes: eventId, eventData, contentHash, userId requis",
        },
        400
      );
    }

    console.log(
      `[Appwrite Function] Début de création pour l'événement ${eventId} par ${userId}`
    );

    // 3. Initialisation du client Appwrite côté serveur
    const client = new Client()
      .setEndpoint(process.env.APPWRITE_ENDPOINT)
      .setProject(process.env.APPWRITE_PROJECT_ID)
      .setKey(process.env.APPWRITE_API_KEY);

    const databases = new Databases(client);

    // 4. Vérification que l'événement n'existe pas déjà
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
        { error: "Cet événement existe déjà", code: "already_exists" },
        409
      );
    } catch (error) {
      if (error.code !== 404) {
        throw error;
      }
      // 404 = document n'existe pas, c'est ce qu'on veut
    }

    // 5. Création de la transaction
    const transaction = await databases.createTransaction();
    console.log(`[Appwrite Function] Transaction créée: ${transaction.$id}`);

    // 6. Préparation des opérations
    const operations = [];

    // Opération 1: Créer le document main
    operations.push({
      action: "create",
      databaseId: process.env.DATABASE_ID,
      collectionId: process.env.COLLECTION_MAIN,
      documentId: eventId,
      data: {
        name: eventData.name || `Événement ${eventId}`,
        originalDataHash: contentHash,
        isActive: true,
        createdBy: userId,
        status: "active",
        error: null,
        allDates: eventData.allDates || [],
      },
      $permissions: [
        `read("user:${userId}")`,
        `update("user:${userId}")`,
        `delete("user:${userId}")`,
      ],
    });

    // Opération 2: Créer tous les produits en bulkCreate
    if (eventData.ingredients && Array.isArray(eventData.ingredients)) {
      operations.push({
        action: "bulkCreate",
        databaseId: process.env.DATABASE_ID,
        collectionId: process.env.COLLECTION_PRODUCTS,
        data: eventData.ingredients.map((ingredient) => ({
          $id: `${ingredient.ingredientHugoUuid}_${eventId}`,
          productHugoUuid:
            ingredient.ingredientHugoUuid || Math.random().toString(36),
          productName: ingredient.ingredientName || "",
          productType: ingredient.ingType || "",
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
          conversionRules: ingredient.conversionRules,
          $permissions: [
            `read("user:${userId}")`,
            `update("user:${userId}")`,
            `delete("user:${userId}")`,
          ],
        })),
      });
    }

    console.log(
      `[Appwrite Function] ${operations.length} opérations préparées`
    );

    // 7. Exécution des opérations
    await databases.createOperations(transaction.$id, operations);
    console.log(`[Appwrite Function] Opérations exécutées avec succès`);

    // 8. Commit de la transaction
    await databases.updateTransaction(transaction.$id, true);
    console.log(
      `[Appwrite Function] Transaction validée avec succès pour ${eventId}`
    );

    return res.json({
      success: true,
      eventId,
      message: "Liste de produits créée avec succès",
    });
  } catch (error) {
    console.error(
      `[Appwrite Function] Erreur lors de la création:`,
      error.message
    );
    console.error("[Appwrite Function] Stack:", error.stack);

    if (error.code === "conflict") {
      return res.json(
        {
          error:
            "Conflit détecté: les données ont été modifiées par une autre opération",
          code: "conflict",
        },
        409
      );
    } else if (error.code === "transaction_limit_exceeded") {
      return res.json(
        {
          error:
            "Limite de transactions dépassée. Veuillez réduire le nombre d'ingrédients",
          code: "transaction_limit_exceeded",
        },
        429
      );
    }

    return res.json(
      {
        error: error.message || "Erreur interne du serveur",
        code: error.code,
        stack: error.stack,
      },
      500
    );
  }
}
