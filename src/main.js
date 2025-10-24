import { Client, Databases } from "node-appwrite";

/**
 * Fonction Appwrite pour créer une liste de produits transactionnelle
 * Variables d'environnement requises:
 * - APPWRITE_API_KEY (clé API avec permissions admin)
 * - APPWRITE_ENDPOINT
 * - APPWRITE_PROJECT_ID
 * - DATABASE_ID
 * - COLLECTION_MAIN
 * - COLLECTION_PRODUCTS
 */

export default async ({ req, res, log, error }) => {
  try {
    log("Début de l'exécution");
    log("Body reçu: " + req.bodyText);

    // 1. Parser les données d'entrée
    if (!req.bodyText) {
      error("Aucun body reçu dans la requête");
      return res.json({ error: "Aucun body reçu" }, 400);
    }

    let inputData;
    try {
      inputData = JSON.parse(req.bodyText);
    } catch (parseError) {
      error("Erreur lors du parsing JSON: " + parseError.message);
      return res.json({ error: "Body JSON invalide" }, 400);
    }

    const { eventId, eventData, contentHash, userId } = inputData;

    log(
      `Données parsées - eventId: ${eventId}, userId: ${userId}, hasEventData: ${!!eventData}`
    );

    // 2. Validation des données d'entrée
    if (!eventId || !eventData || !contentHash || !userId) {
      error(
        `Données manquantes: eventId=${!!eventId}, eventData=${!!eventData}, contentHash=${!!contentHash}, userId=${!!userId}`
      );
      return res.json(
        {
          error:
            "Données manquantes: eventId, eventData, contentHash, userId requis",
        },
        400
      );
    }

    log(
      `Début de création pour l'événement ${eventId} par ${userId}`
    );

    // 3. Initialisation du client Appwrite côté serveur
    // Utiliser la clé API stockée en variable d'environnement
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
      log(`L'événement ${eventId} existe déjà dans main`);
      return res.json(
        { error: "Cet événement existe déjà", code: "already_exists" },
        409
      );
    } catch (checkError) {
      if (checkError.code !== 404) {
        throw checkError;
      }
      // 404 = document n'existe pas, c'est ce qu'on veut
    }

    // 5. Créer le document main
    log("Création du document main...");
    await databases.createDocument(
      process.env.DATABASE_ID,
      process.env.COLLECTION_MAIN,
      eventId,
      {
        name: eventData.name || `Événement ${eventId}`,
        originalDataHash: contentHash,
        isActive: true,
        createdBy: userId,
        status: "active",
        error: null,
        allDates: eventData.allDates || [],
      },
      [
        `read("user:${userId}")`,
        `update("user:${userId}")`,
        `delete("user:${userId}")`,
      ]
    );
    log("Document main créé avec succès");

    // 6. Créer tous les produits en bulk avec createDocuments
    if (eventData.ingredients && Array.isArray(eventData.ingredients)) {
      log(`Création de ${eventData.ingredients.length} produits en bulk...`);
      
      const productsDocuments = eventData.ingredients.map((ingredient) => ({
        $id: `${ingredient.ingredientHugoUuid}_${eventId}`,
        $permissions: [
          `read("user:${userId}")`,
          `update("user:${userId}")`,
          `delete("user:${userId}")`,
        ],
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
      }));
      
      await databases.createDocuments(
        process.env.DATABASE_ID,
        process.env.COLLECTION_PRODUCTS,
        productsDocuments
      );
      
      log(`${eventData.ingredients.length} produits créés avec succès en bulk`);
    }

    return res.json({
      success: true,
      eventId,
      message: "Liste de produits créée avec succès",
    });
  } catch (err) {
    error(`Erreur lors de la création: ${err.message}`);
    error(`Stack: ${err.stack}`);

    if (err.code === "conflict") {
      return res.json(
        {
          error:
            "Conflit détecté: les données ont été modifiées par une autre opération",
          code: "conflict",
        },
        409
      );
    } else if (err.code === "transaction_limit_exceeded") {
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
        error: err.message || "Erreur interne du serveur",
        code: err.code,
      },
      500
    );
  }
};
